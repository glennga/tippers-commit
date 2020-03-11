""" This file contains all participant related functionality. """
import psycopg2.extensions
import communication
import threading
import psycopg2
import protocol
import logging
import socket
import queue

from typing import Any
from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class ParticipantStates(IntEnum):
    """ We define 7 distinct states for a participant. """
    INITIALIZE = 0
    ACTIVE = 1
    PREPARED = 2
    ABORT = 3
    COMMIT = 4
    WAITING = 5
    FINISHED = 6


class TransactionParticipantThread(threading.Thread, communication.GenericSocketUser):
    """
    A participant adheres to the FSM specification below.
    --------------------------------------------------------------------------------------------------------------------
    |    INITIALIZE    | A participant can only enter this state via instantiation (i.e. a call from the server daemon).
    |                  | From this state, the participant must move to the ACTIVE state.
    |------------------|-----------------------------------------------------------------------------------------------|
    |      ACTIVE      | A participant can enter this state from the ACTIVE state or from the INITIALIZE state. Here,
    |                  | insertions are made and acknowledgement messages are sent back to the coordinator informing the
    |                  | success of the insertion itself (insertion-time constraint checking). If an insertion message
    |                  | is given by the coordinator, we cycle back to the ACTIVE state. If a prepare message is given,
    |                  | we send the commit to our RM and reply to the coordinator with our status, moving us to either
    |                  | the ABORT state or the PREPARED state. If an abort message is given, we ABORT and reply to the
    |                  | coordinator with an acknowledgement. If at any point these messages fail (i.e. a coordinator
    |                  | error), we delegate this problem to the ABORT or PREPARED states.
    |------------------|-----------------------------------------------------------------------------------------------|
    |     PREPARED     | A participant can enter this state from the ACTIVE, WAITING, or PREPARED states. Upon
    |                  | receiving a commit message from our coordinator, we move to the COMMIT state. Upon receiving an
    |                  | abort message, we move to the ABORT state. If we experience a socket timeout/error while
    |                  | waiting for a message, we move to the WAITING state. If we exit out of the WAITING state and
    |                  | traverse back to here, we can possibly move from the PREPARED state back to the PREPARED state.
    |------------------|-----------------------------------------------------------------------------------------------|
    |      ABORT       | A participant can enter this state from the following states: RECOVERY, ACTIVE, PREPARED.
    |                  | In ABORT, a participant will always perform an "undo" action from the WAL. If the
    |                  | participant was previously in the RECOVERY state, the participant will move straight to the
    |                  | FINISHED state. Otherwise, a participant will send an acknowledgement to the coordinator. If
    |                  | this action times out or fails, we move to the WAITING state. Otherwise, we move to the
    |                  | FINISHED state.
    |------------------|-----------------------------------------------------------------------------------------------|
    |      COMMIT      | A participant can enter this state from the following states: RECOVERY, PREPARED, WAITING.
    |                  | Here, a completion log is written and persisted. Then, the log is flushed and an
    |                  | acknowledgement is sent to the coordinator. If this fails, we move to the WAITING state.
    |                  | Otherwise, we move to the FINISHED state.
    |------------------|-----------------------------------------------------------------------------------------------|
    |     WAITING      | A participant can enter this state from the following states: PREPARED, ABORT, COMMIT-- if they
    |                  | do not receive a commit or abort message without errors, or if they are unable to send an
    |                  | acknowledgement without errors. In both cases, the participant will close the offending socket.
    |                  | If we are waiting for a COMMIT or ABORT message, we will wait until a new socket is assigned
    |                  | to us by the server daemon and request the decision. If we are waiting to send an
    |                  | acknowledgement of an abort or commit, wait until a new socket is assigned to us and send the
    |                  | acknowledgement. If this fails again, we cycle back to the WAITING state.
    |------------------|-----------------------------------------------------------------------------------------------|
    |     FINISHED     | A participant can only enter this state from the COMMIT state or the ABORT state. This
    |                  | represents the FSM sink. From here, the thread is killed.
    --------------------------------------------------------------------------------------------------------------------
    """

    def __init__(self, transaction_id: str, client_socket: socket.socket, **context):
        communication.GenericSocketUser.__init__(self, client_socket)
        threading.Thread.__init__(self, daemon=True)

        self.protocol_db = protocol.ProtocolDatabase(context['protocol_db'])
        self.transaction_coordinator = context['transaction_coordinator']
        self.context = context

        # Setup a connection to the RM (i.e. Postgres).
        self.conn = psycopg2.connect(
            user=context['postgres_username'],
            password=context['postgres_password'],
            host=context['postgres_hostname'],
            database=context['postgres_database']
        )
        self.transaction_id = psycopg2.extensions.Xid.from_string(transaction_id)
        self.conn.autocommit = False
        self.conn.tpc_begin(self.transaction_id)

        # To enter the PREPARE / ABORT state instead, parent must explicitly change the state after instantiation.
        self.state = ParticipantStates.INITIALIZE
        self.socket_token = queue.Queue(1)
        self.previous_edge_property = None

    def _send_edge(self, content) -> Any:
        self.socket.settimeout(self.context['failure_time'])

        if type(content) == OpCode:
            if not self.send_op(content):
                logger.warning("Unable to send request for transaction status. Moving to WAITING.")
                self.previous_edge_property = content
                self.state = ParticipantStates.WAITING
                return None

            coordinator_response = self.read_message()
            if coordinator_response is None:
                logger.warning("No reply from the coordinator about transaction status. Moving to WAITING.")
                self.previous_edge_property = content
                self.state = ParticipantStates.WAITING

            return coordinator_response

        elif type(content) == ResponseCode:
            coordinator_send = self.send_response(content)
            if not coordinator_send:
                logger.warning("Unable to send response. Moving to WAITING.")
                self.previous_edge_property = content
                self.state = ParticipantStates.WAITING

            return coordinator_send

        else:
            logger.error("Content must be either an OpCode or a ResponseCode.")
            raise RuntimeError("Content must be either an OpCode or a ResponseCode.")

    def _initialize_state(self):
        logger.info(f"New transaction started: {self.transaction_id}.")
        self.protocol_db.log_initialize_of(str(self.transaction_id), TransactionRole.PARTICIPANT)
        self.protocol_db.add_coordinator(str(self.transaction_id), self.transaction_coordinator)
        self.state = ParticipantStates.ACTIVE

    def _active_state(self):
        client_message = self.read_message()
        if client_message is None:
            logger.warning("Socket error occurred while waiting / reading message. Moving to ABORT state.")
            self.state = ParticipantStates.ABORT
            return

        requested_op = client_message[0]
        if requested_op == OpCode.INSERT_FROM_COORDINATOR:
            # We have been issued an INSERT from our coordinator.
            statement = client_message[1]
            if not self._execute_statement(statement):
                logger.warning("Statement was not successfully executed. Moving to ABORT state.")
                self.state = ParticipantStates.ABORT

        elif requested_op == OpCode.PREPARE_TO_COMMIT:
            # We have been asked to prepare to commit. Send the PREPARE to our RM.
            try:
                logger.info("Sending PREPARE to RM.")
                self.conn.tpc_prepare()

                logger.info("RM has approved of PREPARE. Sending PREPARED back, and moving to PREPARE state.")
                self.send_response(ResponseCode.PREPARED_FROM_PARTICIPANT)  # Ignore error here!
                self.state = ParticipantStates.PREPARED

            except Exception as e:
                logger.warning("RM could not PREPARE. Sending ABORT back, and moving to ABORT. Exception message: ", e)
                self.send_response(ResponseCode.ABORT_FROM_PARTICIPANT)  # Ignore error here!
                self.state = ParticipantStates.ABORT

        elif requested_op == OpCode.ROLLBACK_FROM_COORDINATOR:
            # We have been asked to ABORT. Move to the ABORT state.
            self.state = ParticipantStates.ABORT

        else:
            logger.warning(f'Unknown operation received. Ignoring. {client_message}')

    def _prepared_state(self):
        client_message = self.read_message()
        if client_message is None:
            logger.warning("Socket error occurred while waiting / reading message. Moving to WAITING state.")
            self.previous_edge_property = OpCode.TRANSACTION_STATUS
            self.state = ParticipantStates.WAITING
            return

        requested_op = client_message[0]
        if requested_op == OpCode.COMMIT_FROM_COORDINATOR:
            self.state = ParticipantStates.COMMIT
        elif requested_op == OpCode.ROLLBACK_FROM_COORDINATOR:
            self.state = ParticipantStates.ABORT
        else:
            logger.warning(f'Unknown operation received. Ignoring. {client_message}')

    def _abort_state(self):
        logger.info("Sending ROLLBACK to RM.")
        self.conn.tpc_rollback()
        self.protocol_db.log_abort_of(str(self.transaction_id))

        if not self._send_edge(ResponseCode.ACKNOWLEDGE_END):
            logger.warning("Unable to send acknowledgement to coordinator. Moving to WAITING.")

        else:
            self.close()  # Release our resources.
            self.conn.close()
            self.protocol_db.close()
            self.state = ParticipantStates.FINISHED

    def _commit_state(self):
        logger.info("Logging COMMIT and sending COMMIT to local RM. Sending ACK to coordinator.")
        self.conn.tpc_commit()
        self.protocol_db.log_commit_of(str(self.transaction_id))

        if not self._send_edge(ResponseCode.ACKNOWLEDGE_END):
            return  # Coordinator did not acknowledge. Move to WAITING state.

        else:
            self.close()  # Release our resources.
            self.conn.close()
            self.protocol_db.close()
            self.state = ParticipantStates.FINISHED

    def _waiting_state(self):
        self.socket.close()  # We assume this socket to be dead.
        self.socket_token.get()
        logger.info("Moving out of the WAITING state.")
        self.socket_token.task_done()

        # Repeat the action which lead us to the WAITING state.
        coordinator_response = self._send_edge(self.previous_edge_property)
        if coordinator_response is None or not coordinator_response:
            pass  # We have failed in the WAITING state. Looping back.

        elif type(self.previous_edge_property) == ResponseCode and coordinator_response:
            self.close()  # Release our resources.
            self.conn.close()
            self.protocol_db.close()
            self.state = ParticipantStates.FINISHED

        else:
            coordinator_code = coordinator_response[0]
            if coordinator_code == OpCode.COMMIT_FROM_COORDINATOR or \
                    coordinator_code == ResponseCode.TRANSACTION_COMMITTED:
                self.state = ParticipantStates.COMMIT
            elif coordinator_code == OpCode.ROLLBACK_FROM_COORDINATOR or \
                    coordinator_code == ResponseCode.TRANSACTION_ABORTED:
                self.state = ParticipantStates.ABORT
            else:
                logger.error(f"Unknown response received from coordinator: {coordinator_response}.")

    def _execute_statement(self, statement: str) -> bool:
        """ Execute the statement-- send the statement to the RM. If this fails, we return false."""
        try:
            # Execute the statement.
            cur = self.conn.cursor()
            cur.execute(statement)

            statement_no_newlines = statement.replace('\n', '')
            logger.debug(f"{statement_no_newlines} successful.")
            self.send_response(ResponseCode.OK)
            return True

        except psycopg2.IntegrityError as e:
            logger.info("Integrity error caught. Exiting now: ", e)
            return False

        except Exception as e:
            logger.error("Unknown exception caught. Exiting now: ", e)
            return False

    def inject_socket(self, client_socket: socket.socket):
        """ Inject a new socket connection for our participant to use. """
        logger.info(f"Injecting new socket to participant: {client_socket}.")
        self.socket = client_socket
        self.socket_token.put(b'')

    def run(self) -> None:
        while self.state != ParticipantStates.FINISHED:
            if self.state == ParticipantStates.INITIALIZE:
                logger.info("Moving to INITIALIZE state.")
                self._initialize_state()

            elif self.state == ParticipantStates.ACTIVE:
                logger.debug("Moving to ACTIVE state.")
                self._active_state()

            elif self.state == ParticipantStates.PREPARED:
                logger.info("Moving to PREPARED state.")
                self._prepared_state()

            elif self.state == ParticipantStates.ABORT:
                logger.info("Moving to ABORT state.")
                self._abort_state()

            elif self.state == ParticipantStates.COMMIT:
                logger.info("Moving to COMMIT state.")
                self._commit_state()

            elif self.state == ParticipantStates.WAITING:
                logger.info("Moving to WAITING state.")
                self._waiting_state()

        logger.info("Moving to FINISHED state. Exiting thread.")
