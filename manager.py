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
        self.child_threads = []
        self.hostname = hostname
        self.context = context

        threading.Thread.__init__(self, daemon=True)
        GenericSocketUser.__init__(self)

    def _recovery_state(self):
        wal = WriteAheadLogger(self.context['wal_file'])
        for transaction_id in wal.get_uncommitted_transactions():
            # Get our role in this transaction.
            role = wal.get_role_in(transaction_id)

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
                self.child_threads.append(coordinate.TransactionCoordinatorThread(
                    hostname=self.hostname,
                    client_socket=client_socket,
                    **self.context
                ))
                self.child_threads[-1].start()

            elif requested_op == OpCode.INITIATE_PARTICIPANT:
                # Parse the transaction ID from the message.
                transaction_id = client_message[1]

                # We are a part of a transaction that does not originate at this TM. Spawn a participant.
                self.logger.info(f"We are a participant in transaction {transaction_id}. Spawning participant.")
                self.child_threads.append(participate.TransactionParticipantThread(
                    transaction_id=transaction_id,
                    client_socket=client_socket,
                    **self.context
                ))
                self.child_threads[-1].start()

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

        failure_time=manager_json['timeout-s'],
        wal_file=manager_json['wal-file'],
        site_json=c_args.config_path + '/site.json',

        postgres_user=postgres_json['user'],
        postgres_password=postgres_json['password'],
        postgres_host=postgres_json['host'],
        postgres_database=postgres_json['database']
    ).run()
