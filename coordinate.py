""" This file contains all coordinator related functionality. """
import psycopg2.extensions
import communication
import threading
import psycopg2
import protocol
import logging
import socket
import time
import uuid

from multiprocessing.dummy import Pool
from typing import List, Tuple
from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class CoordinatorStates(IntEnum):
    """ We define 7 distinct states for a coordinator. """
    INITIALIZE = 0
    ACTIVE = 1
    POLLING = 2
    ABORT = 3
    COMMIT = 4
    WAITING = 5
    FINISHED = 6


class TransactionCoordinatorThread(threading.Thread, communication.GenericSocketUser):
    """
    A coordinator adheres to the FSM specification below.
    --------------------------------------------------------------------------------------------------------------------
    |    INITIALIZE    | A coordinator can only enter this state via instantiation (i.e. a call from the server daemon).
    |                  | From this state, the coordinator must move to the ACTIVE state.
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

    def __init__(self, site_alias: str, client_socket: socket.socket, **context):
        threading.Thread.__init__(self, daemon=True)
        communication.GenericSocketUser.__init__(self)

        self.protocol_db = protocol.ProtocolDatabase(context['protocol_db'])
        self.socket = client_socket
        self.context = context
        self.active_map = {}

        # Initialize our site-list, which describes our cluster.
        self.site_list = self.context['site_list']
        logger.info(f'Coordinator is aware of site: {self.site_list}')

        # Identify which node we are in our "ring".
        self.node_id = [i for i, j in enumerate(self.site_list) if j['alias'] == site_alias][0]
        logger.info(f'Transaction started at node {self.node_id}.')

        # Setup a connection to the RM (i.e. Postgres).
        self.conn = psycopg2.connect(
            user=self.context['postgres_username'],
            password=self.context['postgres_password'],
            host=self.context['postgres_hostname'],
            database=self.context['postgres_database']
        )

        # To enter the ABORT / POLLING state instead, parent must explicitly change the state after instantiation.
        self.transaction_id = psycopg2.extensions.Xid.from_string(str(uuid.uuid4()))
        self.state = CoordinatorStates.INITIALIZE
        self.previous_state = None

    def _initialize_state(self):
        self.conn.tpc_begin(self.transaction_id)

        logger.info(f"New transaction started: {self.transaction_id}.")
        self.protocol_db.log_initialize_of(str(self.transaction_id), TransactionRole.COORDINATOR)
        self.state = CoordinatorStates.ACTIVE

    def _active_state(self):
        client_message = self.read_message()
        if client_message is None:
            logger.warning("Socket error occurred while waiting / reading message. Moving to ABORT state.")
            self.state = CoordinatorStates.ABORT
            return

        requested_op = client_message[0]
        if requested_op == OpCode.INSERT_FROM_CLIENT:
            statement, hash_input = client_message[2], client_message[3]
            if not self._execute_statement(statement, hash_input):
                logger.warning("Statement was not successfully executed. Moving to ABORT state.")
                self.state = CoordinatorStates.ABORT

        elif requested_op == OpCode.ABORT_TRANSACTION:
            self.state = CoordinatorStates.ABORT

        elif requested_op == OpCode.COMMIT_TRANSACTION:
            try:
                logger.info("Sending PREPARE to RM.")
                self.conn.tpc_prepare()

                if len(self.active_map) != 0:
                    logger.info("RM has approved of PREPARE. Moving to POLLING state.")
                    self.state = CoordinatorStates.POLLING
                else:
                    logger.info("RM has approved of PREPARE and is the sole site. Moving to COMMIT state.")
                    self.state = CoordinatorStates.COMMIT

            except Exception as e:
                logger.warning(f"RM could not PREPARE. Moving to ABORT state. Exception: {e}")
                self.conn.tpc_rollback()
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
        is_successful = thread_pool.starmap(_poll_participants, self.active_map.items())
        thread_pool.close()
        thread_pool.join()

        if all(is_successful):
            logger.info("All participants have agreed to COMMIT. Moving to COMMIT state.")
            self.state = CoordinatorStates.COMMIT
        else:
            logger.info("One or more participants did not agree to COMMIT. Moving to ABORT state.")
            self.state = CoordinatorStates.ABORT

    def _abort_state(self):
        self.protocol_db.log_abort_of(str(self.transaction_id))
        self._final_multicast(OpCode.ROLLBACK_FROM_COORDINATOR)

        if len(self.active_map) != 0:
            self.previous_state = CoordinatorStates.ABORT
            self.state = CoordinatorStates.WAITING
        else:
            self.state = CoordinatorStates.FINISHED

    def _commit_state(self):
        self.protocol_db.log_commit_of(str(self.transaction_id))
        self.conn.tpc_commit()
        logger.info("Sending COMMIT to RM.")
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
                logger.info(f"Connecting to participant {participant_id}.")
                working_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                working_site = self.site_list[participant_id]

                try:
                    self.active_map[participant_id] = working_socket
                    working_socket.connect((working_site['hostname'], working_site['port']))

                except Exception as e:  # Swallow exceptions here. We will attempt to connect at a later time.
                    logger.error(f"Exception caught, but ignoring: {e}")

            # Perform the multicast. Sockets will close upon failure.
            self._final_multicast(OpCode.COMMIT_FROM_COORDINATOR if self.previous_state == CoordinatorStates.COMMIT
                                  else OpCode.ROLLBACK_FROM_COORDINATOR)
            time.sleep(self.context['failure_time'])

        # All participants have acknowledged. Move to FINISHED.
        self.state = CoordinatorStates.FINISHED

    def _finished_state(self):
        self.protocol_db.log_completion_of(str(self.transaction_id))
        self.send_response(ResponseCode.TRANSACTION_COMMITTED if self.previous_state == CoordinatorStates.COMMIT
                           else ResponseCode.TRANSACTION_ABORTED)
        time.sleep(1)  # Wait for client to acknowledge the response.
        self.protocol_db.close()
        self.close()

    def _remove_participants(self, participants_to_remove: List):
        """ Given a list of node-ids, remove the given participants from our active map set. """
        for participant in participants_to_remove:
            self.active_map[participant].close()
            self.active_map.pop(participant)

    def _execute_statement(self, statement: str, hash_input: Tuple) -> bool:
        """ Execute the statement-- send the statement to the RM. If this fails, we return false."""
        endpoint_index = hash(hash_input) % len(self.site_list)
        logger.debug(f"Computed endpoint index {endpoint_index} for statement {statement}.")

        if endpoint_index == self.node_id:
            logger.debug("Insertion is meant to be performed locally. Running statement.")
            try:
                # Execute the statement, if we are the endpoint.
                cur = self.conn.cursor()
                cur.execute(statement)
                logger.debug(f"{statement} successful.")
                self.send_response(ResponseCode.OK)
                return True

            except psycopg2.IntegrityError as e:
                logger.info(f"Integrity error caught. Aborting now: {e}")
                return False

            except Exception as e:
                logger.error(f"Unknown exception caught. Aborting now: {e}")
                return False

        else:
            logger.debug(f"Insertion is to be performed remotely. Issuing statement to participant {endpoint_index}.")
            if endpoint_index not in self.active_map.keys():
                endpoint = self.site_list[endpoint_index]  # Determine entry in site_list.
                logger.debug(f"Endpoint entry is {endpoint}.")

                self.active_map[endpoint_index] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    self.active_map[endpoint_index].connect((endpoint['hostname'], endpoint['port'],))
                    logger.info(f"Adding new participant to transaction: {endpoint['hostname']}")
                except socket.error:
                    logger.error(f"Unable to attach the participant {endpoint['hostname']}.")
                    return False

                self.send_message(OpCode.INITIATE_PARTICIPANT, [str(self.transaction_id), self.node_id],
                                  self.active_map[endpoint_index])

            logger.debug(f"Sending statement {statement} to endpoint {endpoint_index}.")
            self.send_message(OpCode.INSERT_FROM_COORDINATOR, [statement], self.active_map[endpoint_index])

            endpoint_response = self.read_message(self.active_map[endpoint_index])
            logger.debug(f"Received response from endpoint: {endpoint_response}")
            if endpoint_response[0] == ResponseCode.OK:
                self.send_response(ResponseCode.OK)
                return True
            else:
                self.send_response(ResponseCode.FAIL)
                return False

    def _final_multicast(self, op_code: OpCode):
        """ Given our active participants, send an op-code to each. Remove those who acknowledge. """
        participants_to_remove = []  # Avoid in-place deletion while iterating.

        for participant, participant_socket in self.active_map.items():
            logger.info(f"Sending {op_code} to participant {participant}.")
            self.send_message(op_code, [str(self.transaction_id)], participant_socket)  # Swallow the error.

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
