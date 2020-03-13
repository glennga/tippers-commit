""" This script will act as a local TM, which performs 2PC over several sites. """
import __init__  # Stupid way to get logging to work...

import logging.config
import communication
import participate
import coordinate
import threading
import protocol
import psycopg2
import argparse
import logging
import socket
import time
import json
import abc

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


class TransactionRoleAbstractFactory(abc.ABC):
    """ Allows us to observe the effects of faulty participants, coordinators, etc... """

    def __init__(self, **context):
        self.context = context

    @abc.abstractmethod
    def get_coordinator(self, client_socket: Union[socket.socket, None]):
        pass

    @abc.abstractmethod
    def get_participant(self, coordinator_id: int, transaction_id: str, client_socket: socket.socket):
        pass


class TransactionManagerThread(threading.Thread, communication.GenericSocketUser):
    """ The server thread, which is another process's entry point.. """

    class _DefaultTransactionRoleFactory(TransactionRoleAbstractFactory):
        def get_coordinator(self, client_socket: Union[socket.socket, None]):
            return coordinate.TransactionCoordinatorThread(
                client_socket=client_socket,
                **self.context
            )

        def get_participant(self, coordinator_id: int, transaction_id: str, client_socket: socket.socket):
            return participate.TransactionParticipantThread(
                transaction_coordinator=coordinator_id,
                transaction_id=transaction_id,
                client_socket=client_socket,
                **self.context
            )

    def __init__(self, site_alias: str, **context):
        threading.Thread.__init__(self, daemon=True)
        communication.GenericSocketUser.__init__(self)

        self.child_threads = {}
        self.site = site_alias
        self.context = context

        self.site_list = self.context['site_list']
        self.role_factory = TransactionManagerThread._DefaultTransactionRoleFactory(site_alias=site_alias, **context) \
            if 'role_factory' not in context else self.context['role_factory']

        # We automatically start in the RECOVERY state.
        self.state = TransactionManagerStates.RECOVERY

    def _abort_transaction(self, protocol_db: protocol.ProtocolDatabase, transaction_id: str):
        if protocol_db.get_role_in(transaction_id) == TransactionRole.PARTICIPANT:
            # Determine who our coordinator is, and get the transaction status.
            coordinator_id = protocol_db.get_coordinator_for(transaction_id)
            coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            coordinator_socket.connect(
                (self.site_list[coordinator_id]['hostname'], self.site_list[coordinator_id]['port']), )
            logger.info(f"Connecting to coordinator: {self.site_list[coordinator_id]['hostname']}.")

            participant_thread = self.role_factory.get_participant(coordinator_id, transaction_id, coordinator_socket)
            logger.info(f"Spawning participant thread in the ABORT state.")
            participant_thread.state = participate.ParticipantStates.ABORT
            self.child_threads[transaction_id] = participant_thread
            self.child_threads[transaction_id].start()

        else:
            coordinator_thread = self.role_factory.get_coordinator(None)
            coordinator_thread.transaction_id = transaction_id
            logger.info(f"Instantiating coordinator thread in the ABORT state. Not attaching client.")
            coordinator_thread.state = coordinate.CoordinatorStates.ABORT

            # Determine who the participants were in this transaction.
            participant_ids = protocol_db.get_participants_in(transaction_id)
            logger.info(f"We are aware of the following participants {participant_ids}.")
            for participant_id in participant_ids:
                logger.info(f"Connecting to participant {participant_id}.")
                working_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                working_site = self.site_list[participant_id]

                try:
                    coordinator_thread.active_map[participant_id] = working_socket
                    working_socket.connect((working_site['hostname'], working_site['port'],))

                except Exception as e:  # Swallow exceptions here. We will attempt to connect at a later time.
                    logger.error(f"Exception caught, but ignoring: {e}")

            self.child_threads[transaction_id] = coordinator_thread
            self.child_threads[transaction_id].start()

    def _recover_transaction(self, protocol_db: protocol.ProtocolDatabase, transaction_id: str):
        if protocol_db.get_role_in(transaction_id) == TransactionRole.PARTICIPANT:
            # Determine who our coordinator is, and get the transaction status.
            coordinator_id = protocol_db.get_coordinator_for(transaction_id)
            coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            coordinator_socket.connect(
                (self.site_list[coordinator_id]['hostname'], self.site_list[coordinator_id]['port']), )
            logger.info(f"Connecting to coordinator: {self.site_list[coordinator_id]['hostname']}.")

            participant_thread = self.role_factory.get_participant(coordinator_id, transaction_id, coordinator_socket)
            logger.info(f"Spawning participant thread in the PREPARED state.")
            participant_thread.state = participate.ParticipantStates.PREPARED
            self.child_threads[transaction_id] = participant_thread
            self.child_threads[transaction_id].start()

        else:
            coordinator_thread = self.role_factory.get_coordinator(None)
            coordinator_thread.transaction_id = transaction_id
            logger.info(f"Instantiating coordinator thread in the POLLING state. Not attaching client.")
            coordinator_thread.state = coordinate.CoordinatorStates.POLLING

            # Determine who the participants were in this transaction.
            participant_ids = protocol_db.get_participants_in(transaction_id)
            logger.info(f"We are aware of the following participants {participant_ids}.")
            for participant_id in participant_ids:
                logger.info(f"Connecting to participant {participant_id}.")
                working_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                working_site = self.site_list[participant_id]

                try:
                    coordinator_thread.active_map[participant_id] = working_socket
                    working_socket.connect((working_site['hostname'], working_site['port'],))

                except Exception as e:  # Swallow exceptions here. We will attempt to connect at a later time.
                    logger.error(f"Exception caught, but ignoring: {e}")

            self.child_threads[transaction_id] = coordinator_thread
            self.child_threads[transaction_id].start()

    def _recovery_state(self):
        # Initialize our site-list, which describes our cluster.
        logger.info(f"TM is aware of site: {self.site_list}")

        # Setup a connection to the RM (i.e. Postgres).
        recovery_conn = psycopg2.connect(
            user=self.context['postgres_username'],
            password=self.context['postgres_password'],
            host=self.context['postgres_hostname'],
            database=self.context['postgres_database']
        ) if 'test_rm' not in self.context else self.context['test_rm']
        protocol_db = protocol.ProtocolDatabase(self.context['protocol_db'])

        for transaction_id in protocol_db.get_abortable_transactions():
            logger.info(f"Working on to-be-aborted transaction {transaction_id}.")
            self._abort_transaction(protocol_db, transaction_id)

        for transaction_id in recovery_conn.tpc_recover():
            logger.info(f"Working on prepared transaction {transaction_id}.")
            self._recover_transaction(protocol_db, str(transaction_id))

        # We are done with recovery.
        recovery_conn.close()
        protocol_db.close()
        self.state = TransactionManagerStates.INITIALIZE

    def _initialize_state(self):
        # Now, bind and listen on the specified port.
        logger.info(f"Listening for requests through port {self.context['node_port']}.")
        self.socket.bind(('0.0.0.0', self.context['node_port'],))
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
            coordinator_thread = self.role_factory.get_coordinator(client_socket)
            self.child_threads[coordinator_thread.transaction_id] = coordinator_thread
            self.send_message(OpCode.START_TRANSACTION, [str(coordinator_thread.transaction_id)], client_socket)
            coordinator_thread.start()

        elif requested_op == OpCode.INITIATE_PARTICIPANT:
            # Parse the transaction ID from the message.
            transaction_id, coordinator_id = client_message[1:3]

            # We are a part of a transaction that does not originate at this TM. Spawn a participant.
            logger.info(f"We are a participant in transaction {transaction_id}. Spawning participant.")
            self.child_threads[transaction_id] = self.role_factory \
                .get_participant(coordinator_id, transaction_id, client_socket)
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
        logger.info("Starting instance of TM daemon.")
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
    parser.add_argument('site_alias', type=str, help="'site.json' alias of the node (identifies site location).")
    parser.add_argument('--config_path', type=str, default='config', help="Location of the configuration files.")
    c_args = parser.parse_args()

    with open(c_args.config_path + '/manager.json') as manager_config_file:
        manager_json = json.load(manager_config_file)
    with open(c_args.config_path + '/postgres.json') as postgres_config_file:
        postgres_json = json.load(postgres_config_file)
    with open(c_args.config_path + '/site.json') as site_config_file:
        site_json = json.load(site_config_file)

    TransactionManagerThread(
        site_alias=c_args.site_alias,
        node_port=manager_json['node-port'],

        failure_time=manager_json['failure-time'],
        protocol_db=manager_json['protocol-db'],
        site_list=site_json,

        postgres_username=postgres_json['user'],
        postgres_password=postgres_json['password'],
        postgres_hostname=postgres_json['host'],
        postgres_database=postgres_json['database']
    ).run()
