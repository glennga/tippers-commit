""" This script will generate and send TIPPERS transactions to a TM daemon. """
import __init__  # Stupid way to get logging to work...

import communication
import argparse
import datetime
import logging
import socket
import json
import time
import sys

from typing import Tuple, List
from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class _TransactionGenerator(communication.GenericSocketUser):
    def __init__(self, **context):
        self.is_socket_closed = False
        self.context = context
        super().__init__()

    def _insert_statement(self, transaction_id: bytes, statement: str, hash_input: Tuple) -> bool:
        """ Send the insertion to TM. Additionally, send as input the object to hash on (in our case, this is the
        <sensor_id, timestamp>).

        :param transaction_id: ID associated with the current transaction.
        :param statement: INSERT statement to perform.
        :param hash_input: Input to hash our nodes on. This determines where to redirect the request to.
        :return: True if the insertion succeeded. False otherwise.
        """
        logger.debug(f"Sending INSERT to the transaction manager for {transaction_id}.")
        logger.debug(f"Sending statement: {statement}")
        self.send_message(OpCode.INSERT_FROM_CLIENT, [transaction_id, statement, hash_input])

        reply_message = self.read_message()
        logger.debug(f"Received from transaction manager: {reply_message}")
        return reply_message is not None and reply_message[0] == ResponseCode.OK

    def _start_transaction(self) -> bytes:
        """ :return: The transaction ID. """
        if self.is_socket_closed:
            hostname, port = self.context['coordinator_hostname'], int(self.context['coordinator_port'])
            logger.info(f"Socket is closed. Reconnecting to TM at {hostname} through port {port}.")
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((hostname, port))
            self.is_socket_closed = False

        self.send_op(OpCode.START_TRANSACTION)
        transaction_id = self.read_message()[1]
        logger.info(f"Starting transaction. Issued ID: {str(transaction_id)}")
        return transaction_id

    def _abort_transaction(self, transaction_id: bytes):
        logger.info(f"Sending ABORT message to the transaction manager for {transaction_id}.")
        self.send_message(OpCode.ABORT_TRANSACTION, [transaction_id])
        logger.info(f"Received from transaction manager: {self.read_message()}")
        self.is_socket_closed = True
        self.socket.close()

    def _commit_transaction(self, transaction_id: bytes):
        logger.info(f"Sending COMMIT message to the transaction manager for {transaction_id}.")
        self.send_message(OpCode.COMMIT_TRANSACTION, [transaction_id])
        manager_response = self.read_message()
        self.is_socket_closed = True
        self.socket.close()

        if manager_response is None:
            logger.error(f"Connection w/ transaction manager has been lost. Transaction has aborted.")
            self.is_socket_closed = True
            return

        elif manager_response[0] == ResponseCode.TRANSACTION_COMMITTED:
            logger.info(f"Transaction {transaction_id} has been committed.")
        elif manager_response[0] == ResponseCode.TRANSACTION_ABORTED:
            logger.info(f"Transaction {transaction_id} has been aborted.")
        else:
            logger.error(f"Unknown reply from the coordinator. {manager_response}.")

    @staticmethod
    def _convert_timestamp(timestamp):
        timestamp = timestamp.replace(" ", "")
        timestamp = timestamp.replace("'", "")
        timestamp = timestamp[0:10] + " " + timestamp[10:]
        return datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

    def _perform_transaction(self, insert_list: List):
        transaction_id = self._start_transaction()
        for insert in insert_list:
            if not self._insert_statement(transaction_id, insert[0], insert[1]):
                self.socket.close()  # The abort is implicit here.
                return

        self._commit_transaction(transaction_id)

    def _shutdown_manager(self):
        if self.is_socket_closed:
            logger.info(f"Exception caught. Attempting to connect socket again before retry.")
            hostname, port = self.context['coordinator_hostname'], int(self.context['coordinator_port'])
            logger.info(f"Connecting to TM at {hostname} through port {port}.")
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((hostname, port))
            self.is_socket_closed = False

        self.send_op(OpCode.SHUTDOWN)
        logger.info("Waiting 5 seconds for the TM to read our shutdown message.")
        time.sleep(5)

    def __call__(self):
        hostname, port = self.context['coordinator_hostname'], int(self.context['coordinator_port'])
        logger.info(f"Connecting to TM at {hostname} through port {port}.")
        self.socket.connect((hostname, port))

        file_r = open(self.context['benchmark_file'], "r")
        cur_timestamp = self._convert_timestamp(file_r.readline().rstrip().split(",")[-2])
        cur_timestamp = cur_timestamp + datetime.timedelta(seconds=self.context['time_delta'])
        file_r.seek(0)

        sensor_dict = {}
        while True:
            try:
                record = file_r.readline().rstrip()
                if record == "":
                    logger.info('Blank line found. Exiting.')
                    break

                timestamp = self._convert_timestamp(record.split(",")[-2])
                sensor_id = record.split(",")[-1] \
                    .replace(")", "") \
                    .replace(";", "") \
                    .replace("'", "") \
                    .replace(" ", "")

                if timestamp <= cur_timestamp:
                    logger.debug(f'Processing: {record}, ({sensor_id}, {timestamp})')
                    if sensor_id in sensor_dict:
                        sensor_dict[sensor_id].append([record, (sensor_id, timestamp,)])
                    else:
                        sensor_dict[sensor_id] = [[record, (sensor_id, timestamp,)]]

                else:
                    for (k, v) in sensor_dict.items():
                        self._perform_transaction(v)

                    cur_timestamp = cur_timestamp + datetime.timedelta(0, self.context['time_delta'])
                    sensor_dict = {}

            except Exception as e:
                logger.error(f'Exception caught: {e}\n {sys.exc_info()[-1].tb_lineno}')
                break

        # Take care of the remaining items.
        for (k, v) in sensor_dict.items():
            self._perform_transaction(v)

        self._shutdown_manager()
        logger.info("Exiting generator.")
        file_r.close()
        self.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate and submit TIPPERS benchmark transactions.')
    parser.add_argument('--config_path', type=str, default='config', help="Location of the configuration files.")
    c_args = parser.parse_args()

    with open(c_args.config_path + '/generator.json') as generator_config_file:
        generator_json = json.load(generator_config_file)

    _TransactionGenerator(
        coordinator_hostname=generator_json['coordinator-hostname'],
        coordinator_port=generator_json['coordinator-port'],
        benchmark_file=generator_json['benchmark-file'],
        time_delta=generator_json['time-delta']
    )()
