""" This script will act as a local TM, which supports 2PC. """

import participate
import coordinate
import threading
import argparse
import json

from shared import *


class _ServerDaemonThread(threading.Thread, GenericSocketUser):
    """ The server thread, which is another process's entry point.. """
    logger = logging.getLogger(__qualname__)

    def __init__(self, hostname: str, **context):
        self.child_threads = {}
        self.hostname = hostname
        self.context = context

        threading.Thread.__init__(self, daemon=True)
        GenericSocketUser.__init__(self)

    def _recovery_state(self):
        # Initialize our site-list, which describes our cluster.
        with open(self.context['site-json']) as site_config_file:
            site_json = json.load(site_config_file)
        site_list = site_json
        self.logger.info("TM is aware of site: ", site_list)

        wal = WriteAheadLogger(self.context['wal_file'])
        for transaction_id in wal.get_uncommitted_transactions():
            role = wal.get_role_in(transaction_id)

            if role == WriteAheadLogger.Role.PARTICIPANT:
                # Determine who our coordinator is, and get the transaction status.
                coordinator_id = wal.get_coordinator_for(transaction_id)
                coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                coordinator_socket.connect((site_list[coordinator_id].hostname, site_list[coordinator_id].port))
                self.logger.info(f"Connecting to coordinator: {site_list[coordinator_id].hostname}.")

                self.logger.info(f"Spawning participant thread in the RECOVERY state.")
                self.child_threads[transaction_id] = participate.TransactionParticipantThread(
                    transaction_id=transaction_id,
                    client_socket=coordinator_socket,
                    **self.context
                )
                self.child_threads[transaction_id].state = participate.ParticipantStates.RECOVERY
                self.child_threads[transaction_id].start()

            else:
                # Determine who the participants were in this transaction.
                participant_ids = wal.get_participants_in(transaction_id)

                self.logger.info(f"Spawning coordinator thread in the RECOVERY state.")
                # TODO: FINISH

    def run(self) -> None:
        # Before starting, resolve any transactions that haven't been committed.
        self._recovery_state()

        # Now, bind and listen on the specified port.
        self.logger.info(f"Listening for requests through port {int(self.context['node_port'])}.")
        self.socket.bind((socket.gethostname(), self.context['node_port']))
        self.socket.listen(5)

        while True:
            client_socket, client_address = self.socket.accept()
            self.logger.info(f"Connection accepted from {client_address}.")

            # Read the message from the client. Parse the OP code.
            client_message = self.read_message(client_socket)
            requested_op = client_message[0]

            if requested_op == OpCode.NO_OP:
                self.logger.info(f"NO-OP received. Taking no action. :-)")

            elif requested_op == OpCode.START_TRANSACTION:
                # Our transaction originates at this TM. Spawn a separate thread to handle this client.
                self.logger.info("Transaction has been started. Spawning coordinator thread.")
                coordinator_thread = coordinate.TransactionCoordinatorThread(
                    hostname=self.hostname,
                    client_socket=client_socket,
                    **self.context
                )
                self.child_threads[coordinator_thread.transaction_id] = coordinator_thread
                coordinator_thread.start()

            elif requested_op == OpCode.INITIATE_PARTICIPANT:
                # Parse the transaction ID from the message.
                transaction_id = client_message[1]

                # We are a part of a transaction that does not originate at this TM. Spawn a participant.
                self.logger.info(f"We are a participant in transaction {transaction_id}. Spawning participant.")
                self.child_threads[transaction_id] = participate.TransactionParticipantThread(
                    transaction_id=transaction_id,
                    client_socket=client_socket,
                    **self.context
                )
                self.child_threads[transaction_id].start()

            elif requested_op == OpCode.PREPARE_TO_COMMIT or requested_op == OpCode.ABORT_TRANSACTION:
                # Parse the transaction ID from the message.
                transaction_id = client_message[1]
                if transaction_id not in self.child_threads.keys():
                    self.logger.error(f"We are not a participant in transaction {transaction_id}! Ignoring.")
                    continue

                # Connect a new socket to the transaction.
                self.logger.info(f"Attaching new socket to participant thread from transaction {transaction_id}.")
                self.child_threads[transaction_id].inject_socket(client_socket, client_message)

            else:
                self.logger.warning("Unknown/unsupported operation received. Taking no action. ", client_message)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Execute as a local TM daemon.')
    parser.add_argument('hostname', type=str, help="Hostname of the machine. Must match the site file.")
    parser.add_argument('--config_path', type=str, default='config', help="Location of the configuration files.")
    c_args = parser.parse_args()

    with open(c_args.config_path + '/manager.json') as manager_config_file:
        manager_json = json.load(manager_config_file)
    with open(c_args.config_path + '/postgres.json') as postgres_config_file:
        postgres_json = json.load(postgres_config_file)

    _ServerDaemonThread(
        hostname=c_args.hostname,
        node_port=manager_json['port'],

        failure_time=manager_json['timeout'],
        wait_time=manager_json['wait-period'],
        wal_file=manager_json['wal-file'],
        site_json=c_args.config_path + '/site.json',

        postgres_user=postgres_json['user'],
        postgres_password=postgres_json['password'],
        postgres_host=postgres_json['host'],
        postgres_database=postgres_json['database']
    ).run()
