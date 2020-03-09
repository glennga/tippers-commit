""" This script will act as a local TM, which supports 2PC. """
import communication
import participate
import coordinate
import threading
import argparse
import logging
import socket
import recovery
import abc
import time
import json

from typing import Union
from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class TransactionManagerStates(IntEnum):
    """ We define 4 distinct states for a server daemon. """
    RECOVERY = 0
    INITIALIZE = 1
    ACTIVE = 2
    FINISHED = 3


class TransactionStateAbstractFactory(abc.ABC):
    """ Allows us to observe the effects of faulty participants, coordinators, etc... """

    def __init__(self, **context):
        self.context = context

    @abc.abstractmethod
    def get_coordinator(self, client_socket: Union[socket.socket, None]):
        pass

    @abc.abstractmethod
    def get_participant(self, transaction_id: bytes, client_socket: socket.socket):
        pass

    @abc.abstractmethod
    def get_wal(self):
        pass


class _TransactionManagerThread(threading.Thread, communication.GenericSocketUser):
    """ The server thread, which is another process's entry point.. """

    class _DefaultTransactionStateFactory(TransactionStateAbstractFactory):
        def get_coordinator(self, client_socket: Union[socket.socket, None]):
            return coordinate.TransactionCoordinatorThread(
                hostname=self.context['hostname'],
                client_socket=client_socket,
                **self.context
            )

        def get_participant(self, transaction_id: bytes, client_socket: socket.socket):
            return participate.TransactionParticipantThread(
                transaction_id=transaction_id,
                client_socket=client_socket,
                **self.context
            )

        def get_wal(self) -> recovery.WriteAheadLogger:
            return recovery.WriteAheadLogger(self.context['wal_file'])

    def __init__(self, hostname: str, **context):
        threading.Thread.__init__(self, daemon=True)
        communication.GenericSocketUser.__init__(self)

        self.child_threads = {}
        self.hostname = hostname
        self.context = context

        self.site_list = self.context['site_list']
        self.state_factory = _TransactionManagerThread._DefaultTransactionStateFactory(hostname=hostname, **context) \
            if 'state_factory' not in context else self.context['state_factory']

        # We automatically start in the RECOVERY state.
        self.state = TransactionManagerStates.RECOVERY

    def _recovery_state(self):
        # Initialize our site-list, which describes our cluster.
        logger.info(f"TM is aware of site: {self.site_list}")

        wal = self.state_factory.get_wal()
        for transaction_id in wal.get_uncommitted_transactions():
            logger.info(f"Working on uncommitted transaction {transaction_id}.")
            role = wal.get_role_in(transaction_id)

            if role == TransactionRole.PARTICIPANT:
                # Determine who our coordinator is, and get the transaction status.
                coordinator_id = wal.get_coordinator_for(transaction_id)
                coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                coordinator_socket.connect(
                    (self.site_list[coordinator_id]['hostname'], self.site_list[coordinator_id]['port']), )
                logger.info(f"Connecting to coordinator: {self.site_list[coordinator_id]['hostname']}.")

                logger.info(f"Spawning participant thread in the RECOVERY state.")
                participant_thread = self.state_factory.get_participant(transaction_id, coordinator_socket)
                participant_thread.state = participate.ParticipantStates.RECOVERY
                self.child_threads[transaction_id] = participant_thread
                self.child_threads[transaction_id].start()

            else:
                logger.info(f"Instantiating coordinator thread in the RECOVERY state. Not attaching client.")
                coordinator_thread = self.state_factory.get_coordinator(None)
                coordinator_thread.state = coordinate.CoordinatorStates.RECOVERY
                coordinator_thread.transaction_id = transaction_id

                # Determine who the participants were in this transaction.
                participant_ids = wal.get_participants_in(transaction_id)
                logger.info(f"We are aware of the following participants {participant_ids}.")
                for participant_id in participant_ids:
                    logging.info(f"Connecting to participant {participant_id}.")
                    working_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    working_site = self.site_list[participant_id]

                    try:
                        coordinator_thread.active_map[participant_id] = working_socket
                        working_socket.connect((working_site['hostname'], working_site['port'],))

                    except Exception as e:  # Swallow exceptions here. We will attempt to connect at a later time.
                        logger.error(f"Exception caught, but ignoring: {e}")

                self.child_threads[transaction_id] = coordinator_thread
                self.child_threads[transaction_id].start()

        # We are done with recovery.
        self.state = TransactionManagerStates.INITIALIZE

    def _initialize_state(self):
        # Now, bind and listen on the specified port.
        logger.info(f"Listening for requests through port {self.context['node_port']}.")
        self.socket.bind((self.hostname, self.context['node_port'],))
        self.socket.listen(5)

        # Move the the ACTIVE state when we are done.
        self.state = TransactionManagerStates.ACTIVE

    def _active_state(self):
        client_socket, client_address = self.socket.accept()
        logger.info(f"Connection accepted from {client_address}.")

        # Read the message from the client. Parse the OP code.
        client_message = self.read_message(client_socket)
        requested_op = client_message[0]

        if requested_op == OpCode.NO_OP:
            logger.info(f"NO-OP received. Taking no action. :-)")

        elif requested_op == OpCode.SHUTDOWN:
            logger.info(f"Client has informed us to SHUTDOWN. Moving to FINISHED state.")
            self.state = TransactionManagerStates.FINISHED
            client_socket.close()

        elif requested_op == OpCode.START_TRANSACTION:
            # Our transaction originates at this TM. Spawn a separate thread to handle this client.
            logger.info("Transaction has been started. Spawning coordinator thread.")
            coordinator_thread = self.state_factory.get_coordinator(client_socket)
            self.child_threads[coordinator_thread.transaction_id] = coordinator_thread
            coordinator_thread.start()

        elif requested_op == OpCode.INITIATE_PARTICIPANT:
            # Parse the transaction ID from the message.
            transaction_id = client_message[1]

            # We are a part of a transaction that does not originate at this TM. Spawn a participant.
            logger.info(f"We are a participant in transaction {transaction_id}. Spawning participant.")
            self.child_threads[transaction_id] = self.state_factory.get_participant(transaction_id, client_socket)
            self.child_threads[transaction_id].start()

        elif requested_op == OpCode.COMMIT_FROM_COORDINATOR or requested_op == OpCode.ROLLBACK_FROM_COORDINATOR:
            # Parse the transaction ID from the message.
            transaction_id = client_message[1]

            if transaction_id not in self.child_threads.keys():
                logger.info(f"We have no knowledge of transaction {transaction_id}. Replying with ACK.")
                self.send_response(ResponseCode.ACKNOWLEDGE_END, client_socket)
                time.sleep(0.5)  # Wait for coordinator to receive our acknowledgement.
                client_socket.close()

            else:
                logger.info(f"Injecting socket {client_socket} to participant {self.child_threads[transaction_id]}.")
                self.child_threads[transaction_id].inject_socket(client_socket)

        else:
            logger.warning(f"Unknown/unsupported operation received. Taking no action. {client_message}")

    def run(self) -> None:
        while self.state != TransactionManagerStates.FINISHED:
            if self.state == TransactionManagerStates.RECOVERY:
                logger.info("Moving to RECOVERY state.")
                self._recovery_state()

            elif self.state == TransactionManagerStates.INITIALIZE:
                logger.info("Moving to INITIALIZE state.")
                self._initialize_state()

            elif self.state == TransactionManagerStates.ACTIVE:
                logger.info("Moving to ACTIVE state.")
                self._active_state()

        logger.info("Exiting TM.")
        self.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Execute as a local TM daemon.')
    parser.add_argument('hostname', type=str, help="Hostname of the machine. Must match the site file.")
    parser.add_argument('--config_path', type=str, default='config', help="Location of the configuration files.")
    c_args = parser.parse_args()

    with open(c_args.config_path + '/manager.json') as manager_config_file:
        manager_json = json.load(manager_config_file)
    with open(c_args.config_path + '/postgres.json') as postgres_config_file:
        postgres_json = json.load(postgres_config_file)
    with open(c_args.config_path + 'site.json') as site_config_file:
        site_json = json.load(site_config_file)

    _TransactionManagerThread(
        hostname=c_args.hostname,
        node_port=manager_json['port'],

        failure_time=manager_json['timeout'],
        wait_time=manager_json['wait-period'],
        wal_file=manager_json['wal-file'],
        site_json=site_json,

        postgres_user=postgres_json['user'],
        postgres_password=postgres_json['password'],
        postgres_host=postgres_json['host'],
        postgres_database=postgres_json['database']
    ).run()
