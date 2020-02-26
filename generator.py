""" This script will generate and send TIPPERS transactions to a TM daemon. """

import argparse
import json

from shared import *


class _TransactionGenerator(GenericSocketUser):
    def __init__(self, **kwargs):
        super().__init__()
        self.kwargs = kwargs

    def _insert_statement(self, transaction_id: bytes, statement: str, hash_input) -> bool:
        """ Send the insertion to TM. Additionally, send as input the object to hash on (in our case, this is the
        <sensor_id, timestamp>).

        :param transaction_id: ID associated with the current transaction.
        :param statement: INSERT statement to perform.
        :param hash_input: Input to hash our nodes on. This determines where to redirect the request to.
        :return: True if the insertion succeeded. False otherwise.
        """
        logging.debug(f"Sending INSERT to the transaction manager for transaction {transaction_id}.")
        logging.debug(f"Sending statement: {statement}")
        self.send_message(OpCode.INSERT, [transaction_id, statement, hash_input])

        reply_message = self.read_message()
        logging.debug("Received from transaction manager: ", reply_message)
        return reply_message == ResponseCode.OK

    def _start_transaction(self) -> bytes:
        """ :return: The transaction ID. """
        self.send_op(OpCode.START_TRANSACTION)
        transaction_id = self.read_message()
        logging.info(f"Starting transaction. Issued ID: ", str(transaction_id))
        return transaction_id

    def _abort_transaction(self, transaction_id: bytes):
        """ Abort all transactions up until this point. """
        logging.info(f"Sending ABORT message to the transaction manager for transaction {transaction_id}.")
        self.send_message(OpCode.ABORT_TRANSACTION, [transaction_id])
        logging.info("Received from transaction manager: ", self.read_message())

    def _commit_transaction(self, transaction_id: bytes):
        """ Commit all transactions up until this point. """
        logging.info(f"Sending COMMIT message to the transaction manager for transaction {transaction_id}.")
        self.send_message(OpCode.COMMIT_TRANSACTION, [transaction_id])
        logging.info("Received from transaction manager: ", self.read_message())

    def __call__(self):
        logging.info(f"Connecting to TM at {self.kwargs['server']} through port {int(self.kwargs['port'])}.")

        # Complete this portion-- note: we are doing hashing at the TM instead of the client now...
        filename = self.kwargs['benchmark-file']
        while True:
            pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate and submit TIPPERS benchmark transactions.')
    parser.add_argument('--config_path', type=str, default='config', help="Location of the configuration files.")
    c_args = parser.parse_args()

    with open(c_args.config_path + 'generator.json') as generator_config_file:
        generator_json = json.load(generator_config_file)

    _TransactionGenerator(**generator_json)()
