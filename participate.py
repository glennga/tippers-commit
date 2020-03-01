""" This file contains all participant related functionality. """
import threading
import psycopg2
import time

from shared import *


class ParticipantStates(IntEnum):
    """ We define 6 states for a participant. """
    INITIALIZATION = 0
    OPERATIONAL = 1
    PREPARED = 2
    RECOVERY = 3
    WAITING = 4
    FINISHED = 5


class TransactionParticipantThread(threading.Thread, GenericSocketUser):
    logger = logging.getLogger(__qualname__)

    def __init__(self, transaction_id: bytes, client_socket: socket.socket, **context):
        threading.Thread.__init__(self, daemon=True)
        GenericSocketUser.__init__(self)

        self.wal = WriteAheadLogger(context['wal_file'])
        self.transaction_id = transaction_id
        self.socket = client_socket
        self.context = context

        # Setup a connection to the RM (i.e. Postgres).
        self.conn = psycopg2.connect(
            user=context['postgres_username'],
            password=context['postgres_password'],
            host=context['postgres_hostname'],
            database=context['postgres_database']
        )
        self.conn.autocommit = False

        # These are used when a socket fails.
        self.socket.settimeout(context['failure_time'])
        self.message_from_parent = None

        # To enter the RECOVERY state instead, parent must explicitly change this after instantiation.
        self.state = ParticipantStates.INITIALIZATION

    def _abort_action(self):
        """ Undo each INSERT from most to least recent. """
        self.logger.info("Rolling back all uncommitted statements from our WAL.")
        for statement in self.wal.get_undo_for(self.transaction_id):
            try:
                cur = self.conn.cursor()
                cur.execute(statement)
                self.logger.debug(f"{statement} successful.")

            except Exception as e:
                self.logger.fatal("Exception caught. Exiting now. Database is in a corrupted state!", e)
                self.state = ParticipantStates.FINISHED
                return

        # Commit this work to the RM.
        self.state = ParticipantStates.FINISHED
        self.conn.commit()

    def _waiting_state(self):
        """ Close our socket and wait for our parent thread to attach a new connection. """
        while self.message_from_parent is None:
            time.sleep(self.context['wait_time'])
        self.logger.info("Moving out of the WAITING state.")

        # From the WAITING state, we can only move to PREPARED or FINISHED state if we are told to abort.
        if self.message_from_parent[0] == OpCode.PREPARE_TO_COMMIT:
            self.logger.info("Moving to the PREPARE state.")
            self._prepared_state(self.message_from_parent)

        elif self.message_from_parent[0] == OpCode.ABORT_TRANSACTION:
            self.logger.info("Coordinator has informed us that we must ABORT.")
            self._abort_action()
            self.state = ParticipantStates.FINISHED

        else:
            self.logger.warning("Unknown operation received. Ignoring and moving to FINISHED state.")
            self.state = ParticipantStates.FINISHED

        self.message_from_parent = None

    def _recovery_state(self):
        """ Redo each INSERT from least to most recent, if we receive a COMMIT status from our coordinator.  """
        self.send_op(OpCode.TRANSACTION_STATUS)
        coordinator_response = self.read_message()

        if coordinator_response[0] == ResponseCode.TRANSACTION_COMMITTED:
            self.logger.info("Re-performing all uncommitted statements from our WAL.")
            for statement in self.wal.get_redo_for(self.transaction_id):
                try:
                    cur = self.conn.cursor()
                    cur.execute(statement)
                    self.logger.debug(f"{statement} successful.")

                except Exception as e:
                    self.logger.fatal("Exception caught. Exiting now. Database is in a corrupted state!", e)
                    sys.exit(0)

            # Commit this work to the RM.
            self.conn.commit()

        elif coordinator_response[0] == ResponseCode.TRANSACTION_ABORTED:
            self._abort_action()
            self.state = ParticipantStates.FINISHED

        else:
            self.logger.error('Unknown operation received. Ignoring. ', coordinator_response)

    def _operational_state(self, client_message: List):
        requested_op = client_message[0]
        if requested_op == OpCode.INSERT_FROM_COORDINATOR:
            # We have been issued an INSERT from our coordinator.
            statement = client_message[2]
            if not self.execute_statement(statement):
                self.state = ParticipantStates.WAITING

        elif requested_op == OpCode.PREPARE_TO_COMMIT:
            # We have been asked to prepare to commit. Send the commit to our RM.
            try:
                self.logger.info("Flushing WAL to disk, then sending the COMMIT message to the RM.")
                self.wal.flush_log()
                self.conn.commit()

                self.logger.info("RM has approved of COMMIT. Sending PREPARE back to coordinator.")
                is_send_success = self.send_response(ResponseCode.PREPARED_FROM_PARTICIPANT)
                self.state = ParticipantStates.WAITING if not is_send_success else ParticipantStates.PREPARED

            except Exception as e:
                is_send_success = self.send_response(ResponseCode.ABORT_FROM_PARTICIPANT)
                self.logger.warning("RM could not commit. Sending ABORT back to coordinator.")
                self.logger.warning("Exception message: ", e)

                if is_send_success:
                    self._abort_action()
                self.state = ParticipantStates.WAITING if not is_send_success else ParticipantStates.FINISHED

        elif requested_op == OpCode.ABORT_TRANSACTION:
            # We have been asked to ABORT. Flush our logs to disk, rollback, and reply with an ACK.
            self.logger.info('Flushing the WAL to disk.')
            self.wal.flush_log()
            self._abort_action()
            self.conn.close()

            # Log ABORT to WAL, and close.
            self.wal.log_abort_of(self.transaction_id)
            self.wal.close()

            self.logger.info('Acknowledging ABORT. Sending ACK back to the coordinator.')
            self.state = ParticipantStates.WAITING if not self.send_response(
                ResponseCode.ACKNOWLEDGE_END) else ParticipantStates.FINISHED

        else:
            self.logger.warning('Unknown operation received. Ignoring. ', client_message)

    def _prepared_state(self, client_message: List):
        requested_op = client_message[0]
        if requested_op == OpCode.COMMIT_FROM_COORDINATOR:
            # We have been asked to commit. Write "COMPLETION" to our log and reply with an ACK.
            self.wal.log_commit_of(self.transaction_id)
            self.conn.close()
            self.wal.close()

            self.state = ParticipantStates.WAITING if not self.send_response(
                ResponseCode.ACKNOWLEDGE_END) else ParticipantStates.FINISHED
            self.logger.info('Acknowledging COMMIT. Sent ACK back to the coordinator.')

        elif requested_op == OpCode.ROLLBACK_FROM_COORDINATOR:
            # We have been asked to rollback. Flush our logs to disk, rollback, and reply with an ACK.
            self.logger.info('Flushing the WAL to disk.')
            self.wal.flush_log()
            self._abort_action()
            self.conn.close()

            # Log ABORT to WAL, and close.
            self.wal.log_abort_of(self.transaction_id)
            self.wal.close()

            self.state = ParticipantStates.WAITING if not self.send_response(
                ResponseCode.ACKNOWLEDGE_END) else ParticipantStates.FINISHED
            self.logger.info('Acknowledging ABORT. Sending ACK back to the coordinator.')

        else:
            self.logger.warning('Unknown operation received. Ignoring. ', client_message)

    def execute_statement(self, statement: str):
        """ Execute the statement-- send the statement to the RM. If this fails, we kill ourselves. """
        try:
            # Execute the statement.
            cur = self.conn.cursor()
            cur.execute(statement)

            # Write this operation to our log.
            self.wal.log_statement(self.transaction_id, statement)
            self.logger.debug(f"{statement} successful.")
            self.send_response(ResponseCode.OK)

        except psycopg2.IntegrityError as e:
            self.logger.info("Integrity error caught. Exiting now: ", e)
            self.send_response(ResponseCode.FAIL)
            sys.exit(0)

        except Exception as e:
            self.logger.error("Unknown exception caught. Exiting now: ", e)
            self.send_response(ResponseCode.FAIL)
            sys.exit(0)

    def inject_socket(self, client_socket: socket.socket, client_message: List):
        """ Inject a new socket connection for our participant to use. """
        self.socket = client_socket
        self.message_from_parent = client_message

    def run(self) -> None:
        if self.state == ParticipantStates.RECOVERY:
            # If specified by our parent thread, enter the recovery state.
            self._recovery_state()
            return

        # Otherwise, transition to the insert state.
        self.state = ParticipantStates.OPERATIONAL

        while True:
            # Read the message from the client. Parse the OP code.
            client_message = self.read_message()
            requested_op = client_message[0]
            message_transaction_id = client_message[1]

            # Verify that we are working within the same transaction.
            if message_transaction_id != self.transaction_id:
                self.logger.fatal(f"Transaction ID mismatch! Expected {self.transaction_id}, but received "
                                  f"{message_transaction_id}.")
                continue

            if requested_op == OpCode.NO_OP:
                self.logger.info(f"NO-OP received. Taking no action. :-)")
                continue

            if self.state == ParticipantStates.OPERATIONAL:
                self._operational_state(client_message)

            elif self.state == ParticipantStates.PREPARED:
                self._prepared_state(client_message)

            elif self.state == ParticipantStates.WAITING:
                self._waiting_state()

            else:
                self.logger.info(f"No longer in transitional state. Exiting now from {self.state}.")
                return
