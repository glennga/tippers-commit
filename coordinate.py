""" This file contains all coordinator related functionality. """
import communication
import recovery
import logging
import socket
import threading
import psycopg2
import time
import uuid

from multiprocessing.dummy import Pool
from typing import List
from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class CoordinatorStates(IntEnum):
    """ We define 8 distinct states for a coordinator. """
    INITIALIZE = 0
    RECOVERY = 1
    ACTIVE = 2
    POLLING = 3
    ABORT = 4
    COMMIT = 5
    WAITING = 6
    FINISHED = 7


class TransactionCoordinatorThread(threading.Thread, communication.GenericSocketUser):
    """
    A coordinator adheres to the FSM specification below.
    --------------------------------------------------------------------------------------------------------------------
    |    INITIALIZE    | A coordinator can only enter this state via instantiation (i.e. a call from the server daemon).
    |                  | From this state, the coordinator must move to the ACTIVE state.
    |------------------|-----------------------------------------------------------------------------------------------|
    |     RECOVERY     | A coordinator can only enter this state by explicitly setting the state variable to RECOVERY.
    |                  | This informs the coordinator that they should perform a "redo -> undo" action from the WAL. We
    |                  | will then check the state of this transaction from the WAL. If there exists no COMMIT message,
    |                  | we move to the ABORT state. If there exists a COMMIT message but no COMPLETION message, move
    |                  | to the COMMIT state.
    |------------------|-----------------------------------------------------------------------------------------------|
    |      ACTIVE      | A coordinator can enter this state from the ACTIVE state or from the INITIALIZE state. Here,
    |                  | insertions are made at the local site or by sending insertions to various participants and
    |                  | waiting for acknowledgements. If a PREPARE message is given by the client, we send the commit
    |                  | to our local RM. If this succeeds, then we move to the POLLING state. Otherwise, we move to the
    |                  | ABORT state. If we fail to get an acknowledgement back from a participant, then we move to the
    |                  | ABORT state.
    |------------------|-----------------------------------------------------------------------------------------------|
    |     POLLING      | A coordinator can enter this state from the ACTIVE state. Here, we send a "prepare" message to
    |                  | all participants. If we fail to receive an acknowledgement from any participant or at-least
    |                  | one participant replies back with "abort", then move to the ABORT state. Otherwise, we move to
    |                  | the COMMIT state.
    |------------------|-----------------------------------------------------------------------------------------------|
    |      ABORT       | A coordinator can enter this state from the following states: RECOVERY, ACTIVE, POLLING.
    |                  | In ABORT, a coordinator will always perform an "undo" action from the WAL. In this state, a
    |                  | coordinator will send an ABORT message to all participants and await acknowledgements from
    |                  | each. If there exists a socket error, we will swallow the errors (temporarily) and send abort
    |                  | messages to the remaining participants. Noting the participants that did not acknowledge the
    |                  | abort, we move to the WAITING state. If all participants acknowledged the abort, then we move
    |                  | to the FINISHED state.
    |------------------|-----------------------------------------------------------------------------------------------|
    |      COMMIT      | A coordinator can enter this state from the following states: RECOVERY or POLLING. Here, we
    |                  | send a "commit" message to all participants involved and await acknowledgements from each. If
    |                  | there exists a socket error, we will swallow the errors (temporarily) and send commit
    |                  | messages to the remaining participants. Noting the participants that did not acknowledge the
    |                  | commit, we move to the WAITING state. If all participants acknowledged the commit, then we move
    |                  | to the FINISHED state.
    |------------------|-----------------------------------------------------------------------------------------------|
    |     WAITING      | A coordinator can enter this state from the following states: ABORT or COMMIT. In both cases,
    |                  | we note the participants that didn't acknowledge the decision and periodically send "abort" /
    |                  | "commit"  to these participants. Upon receiving acknowledgements from all, we move to the
    |                  | FINISHED state.
    |------------------|-----------------------------------------------------------------------------------------------|
    |     FINISHED     | A participant can only enter this state from the COMMIT state or the ABORT state. Here, a
    |                  | completion log is written and persisted. This represents the FSM sink. From here, the thread
    |                  | is killed.
    --------------------------------------------------------------------------------------------------------------------
    """

    @staticmethod
    def _generate_transaction_id() -> bytes:
        new_transaction_id = uuid.uuid1().bytes
        logger.info(f"New transaction ID generated: {new_transaction_id}")
        return new_transaction_id

    def __init__(self, hostname: str, client_socket: socket.socket, **context):
        threading.Thread.__init__(self, daemon=True)
        communication.GenericSocketUser.__init__(self)

        self.wal = recovery.WriteAheadLogger(context['wal_file'])
        self.transaction_id = self._generate_transaction_id()
        self.socket = client_socket
        self.context = context
        self.active_map = {}

        # Setup a connection to the RM (i.e. Postgres).
        self.conn = psycopg2.connect(
            user=self.context['postgres_username'],
            password=self.context['postgres_password'],
            host=self.context['postgres_hostname'],
            database=self.context['postgres_database']
        )
        self.conn.autocommit = False

        # Initialize our site-list, which describes our cluster.
        self.site_list = self.context['site_json']
        logging.info('Coordinator is aware of site: ', self.site_list)

        # Identify which node we are in our "ring".
        self.node_id = [i for i, j in enumerate(self.site_list) if j.hostname == hostname][0]
        logging.info(f'Transaction started at node {self.node_id}.')

        # To enter the RECOVERY state instead, parent must explicitly change the state after instantiation.
        self.state = CoordinatorStates.INITIALIZE
        self.previous_state = None

    def _initialize_state(self):
        self.wal.initialize_transaction(self.transaction_id, TransactionRole.COORDINATOR, self.node_id)
        self.state = CoordinatorStates.ACTIVE

    def _recovery_state(self):
        if self.wal.is_transaction_prepared(self.transaction_id):  # TODO: ????
            logger.info("Undoing all uncommitted statements from the WAL.")
            for statement in self.wal.get_undo_for(self.transaction_id):
                try:
                    cur = self.conn.cursor()
                    cur.execute(statement)
                    logger.debug(f"{statement} successful.")

                except Exception as e:  # We should never reach here.
                    logger.fatal("Exception caught! Fatal! ", e)

            logger.info("Re-performing all uncommitted statements from our WAL.")
            for statement in self.wal.get_redo_for(self.transaction_id):
                try:
                    cur = self.conn.cursor()
                    cur.execute(statement)
                    logger.debug(f"{statement} successful.")

                except Exception as e:  # We should never reach here.
                    logger.fatal("Exception caught! Fatal! ", e)

            # Move to the COMMIT state.
            self.state = CoordinatorStates.COMMIT

        else:
            # If a transaction does not have a COMMIT record, we can safely abort.
            self.state = CoordinatorStates.ABORT
            return

    def _active_state(self):
        client_message = self.read_message()
        if client_message is None:
            logger.warning("Socket error occurred while waiting / reading message. Moving to ABORT state.")
            self.state = CoordinatorStates.ABORT
            return

        requested_op = client_message[0]
        if requested_op == OpCode.INSERT_FROM_CLIENT:
            # We have been an issued an INSERT from our client.
            statement = client_message[2]
            hash_input = client_message[3]
            if not self._execute_statement(statement, hash_input):
                logger.warning("Statement was not successfully executed. Moving to ABORT state.")
                self.state = CoordinatorStates.ABORT

        elif requested_op == OpCode.ABORT_TRANSACTION:
            # We have been issued an ABORT from our client.
            self.state = CoordinatorStates.ABORT

        elif requested_op == OpCode.COMMIT_TRANSACTION:
            # We have been issued a COMMIT from our client. Send this to our RM first.
            try:
                logger.info("Sending COMMIT to RM.")  # TODO: ????
                self.conn.commit()

                logger.info("RM has approved of COMMIT. Flushing WAL and moving to POLLING state.")
                self.wal.log_commit_of(self.transaction_id)
                self.wal.flush_log()
                self.state = CoordinatorStates.POLLING

            except Exception as e:
                logger.warning("RM could not commit. Flushing WAL and moving to ABORT state. Exception: ", e)
                self.wal.log_abort_of(self.transaction_id)
                self.wal.flush_log()
                self.state = CoordinatorStates.ABORT

        else:
            logger.warning('Unknown operation received. Ignoring. ', client_message)

    def _polling_state(self):
        def _poll_participants(participant: int, participant_socket: socket.socket) -> bool:
            logger.info(f"Sending PREPARE to participant {participant}.")
            if not self.send_op(OpCode.PREPARE_TO_COMMIT, participant_socket):
                return False

            participant_response = self.read_message(participant_socket)
            return participant_response is not None and participant_response[0] == \
                   ResponseCode.PREPARED_FROM_PARTICIPANT

        thread_pool = Pool(4)  # For now, this is a hard-coded value.
        logger.info("Now polling participants (moving polling to thread pool).")
        is_successful = thread_pool.map(_poll_participants, self.active_map)
        thread_pool.join()
        thread_pool.close()

        if all(is_successful):
            logger.info("All participants have agreed to COMMIT. Moving to COMMIT state.")
            self.state = CoordinatorStates.COMMIT
        else:
            logger.info("One or more participants did not agree to COMMIT. Moving to ABORT state.")
            self.state = CoordinatorStates.ABORT

    def _abort_state(self):
        logger.info("Rolling back all uncommitted statements from our WAL.")
        for statement in self.wal.get_undo_for(self.transaction_id):
            try:
                cur = self.conn.cursor()
                cur.execute(statement)
                logger.debug(f"{statement} successful.")

            except Exception as e:
                logger.warning("Exception caught, but ignoring: ", e)

        # Verify that the log of the abort is persisted.
        self.wal.log_abort_of(self.transaction_id)
        self.wal.flush_log()

        self._final_multicast(OpCode.ROLLBACK_FROM_COORDINATOR)
        if len(self.active_map) != 0:
            self.previous_state = CoordinatorStates.COMMIT
            self.state = CoordinatorStates.WAITING
        else:
            self.state = CoordinatorStates.FINISHED

    def _commit_state(self):
        # Perform our local COMMIT work.
        self.wal.log_commit_of(self.transaction_id)
        self.wal.flush_log()

        # Broadcast this to all participants.
        self._final_multicast(OpCode.COMMIT_FROM_COORDINATOR)
        if len(self.active_map) != 0:
            self.previous_state = CoordinatorStates.COMMIT
            self.state = CoordinatorStates.WAITING
        else:
            self.state = CoordinatorStates.FINISHED

    def _waiting_state(self):
        while len(self.active_map) != 0:
            logger.info("Reconnecting disconnected participants.")

            for participant_id in self.active_map.keys():
                logging.info(f"Connecting to participant {participant_id}.")
                working_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                working_site = self.site_list[participant_id]

                try:
                    self.active_map[participant_id] = working_socket
                    working_socket.connect((working_site['hostname'], working_site['port']))

                except Exception as e:  # Swallow exceptions here. We will attempt to connect at a later time.
                    logger.error(f"Exception caught, but ignoring: {e}")

            # Perform the multicast. Sockets will close upon failure.
            self._final_multicast(OpCode.COMMIT_FROM_COORDINATOR if self.previous_state == CoordinatorStates.COMMIT else
                                  OpCode.ROLLBACK_FROM_COORDINATOR)
            time.sleep(self.context['wait_time'])

        # All participants have acknowledged. Move to FINISHED.
        self.state = CoordinatorStates.FINISHED

    def _finished_state(self):
        self.wal.log_completion_of(self.transaction_id)
        self.wal.flush_log()
        self.wal.close()
        self.close()

    def _remove_participants(self, participants_to_remove: List):
        """ Given a list of node-ids, remove the given participants from our active map set. """
        for participant in participants_to_remove:
            self.active_map[participant].close()
            self.active_map.pop(participant)

    def _execute_statement(self, statement: str, hash_input: List) -> bool:
        """ Execute the statement-- send the statement to the RM. If this fails, we return false."""
        endpoint = self.site_list[hash(hash_input) % len(self.site_list)]

        if endpoint == self.node_id:
            logger.debug("Insertion is meant to be performed locally. Running statement.")
            try:
                # Execute the statement, if we are the endpoint.
                cur = self.conn.cursor()
                cur.execute(statement)

                # Write this operation to our log.
                self.wal.log_statement(self.transaction_id, statement)
                logger.debug(f"{statement} successful.")
                self.send_response(ResponseCode.OK)
                return True

            except psycopg2.IntegrityError as e:
                logger.info("Integrity error caught. Aborting now: ", e)
                return False

            except Exception as e:
                logger.error("Unknown exception caught. Aborting now: ", e)
                return False

        else:
            logger.debug(f"Insertion is to be performed remotely. Issuing statement to participant {endpoint}.")
            if endpoint not in self.active_map.keys():
                self.active_map[endpoint] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.active_map[endpoint].connect((self.site_list[endpoint].hostname, self.site_list[endpoint].port,))
                logging.info("Adding new participant to transaction: ", self.site_list[endpoint].hostname)
                self.send_message(OpCode.INITIATE_PARTICIPANT, [self.transaction_id], self.active_map[endpoint])

            logging.debug(f"Sending statement {statement} to endpoint {endpoint}.")
            self.send_message(OpCode.INSERT_FROM_COORDINATOR, [statement])

            endpoint_response = self.read_message(self.active_map[endpoint])
            logging.debug(f"Received response from endpoint: ", endpoint_response)
            return endpoint[0] == ResponseCode.OK

    def _final_multicast(self, op_code: OpCode):
        """ Given our active participants, send an op-code to each. Remove those who acknowledge. """
        participants_to_remove = []  # Avoid in-place deletion while iterating.

        for participant, participant_socket in self.active_map:
            logger.info(f"Sending {op_code} to participant {participant}.")
            self.send_op(op_code, participant_socket)  # Swallow the error.

            participant_response = self.read_message(participant_socket)
            if participant_response is not None and participant_response[0] == ResponseCode.ACKNOWLEDGE_END:
                logger.info(f"Participant {participant} has acknowledged {op_code}.")
                logger.info(f"Removing participant {participant} from active site map.")
                participants_to_remove.append(participant)
            else:
                logger.warning(f"Participant {participant} has not acknowledged {op_code}.")

        self._remove_participants(participants_to_remove)

    def run(self) -> None:
        while self.state != CoordinatorStates.FINISHED:
            if self.state == CoordinatorStates.INITIALIZE:
                logger.info("Moving to INITIALIZE state.")
                self._initialize_state()

            elif self.state == CoordinatorStates.RECOVERY:
                logger.info("Moving to RECOVERY state.")
                self._recovery_state()

            elif self.state == CoordinatorStates.ACTIVE:
                logger.info("Moving to ACTIVE state.")
                self._active_state()

            elif self.state == CoordinatorStates.POLLING:
                logger.info("Moving to POLLING state.")
                self._polling_state()

            elif self.state == CoordinatorStates.ABORT:
                logger.info("Moving to ABORT state.")
                self._abort_state()

            elif self.state == CoordinatorStates.WAITING:
                logger.info("Moving to WAITING state.")
                self._waiting_state()

            elif self.state == CoordinatorStates.COMMIT:
                logger.info("Moving to COMMIT state.")
                self._commit_state()

        logger.info("Moving to FINISHED state. Exiting thread.")
        self._finished_state()
