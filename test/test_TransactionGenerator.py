import threading
import socket
import logging
import unittest
import time
import communication

from coordinate import TransactionCoordinatorThread
from generator import _TransactionGenerator
from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class TestTransactionGenerator(unittest.TestCase):
    """ Verifies the class that submits transactions to our TM. We are testing from the TM side (non-invasive). """
    test_port = 55511

    @staticmethod
    def _generator_wrapper(port):
        time.sleep(1)  # Must wait for parent...
        _TransactionGenerator(
            coordinator_hostname=socket.gethostname(),
            coordinator_port=port,
            benchmark_file="resources/test.workload",
            time_delta=60000
        )()

    def test_happy_path(self):
        manager_socket = communication.GenericSocketUser()
        manager_socket.socket.bind((socket.gethostname(), self.test_port))
        manager_socket.socket.listen(5)

        # Spawn our generator thread.
        generator_thread = threading.Thread(target=self._generator_wrapper, args=(self.test_port,))
        generator_thread.start()

        # Accept our generator connection.
        generator_socket, generator_address = manager_socket.socket.accept()
        logger.info(f"Connection accepted from {generator_address}.")

        transaction_id_set = []
        while True:
            generator_message = manager_socket.read_message(generator_socket)
            logger.info(f"Received message: {generator_message} from generator.")

            if generator_message[0] == OpCode.START_TRANSACTION:
                transaction_id_set.append(TransactionCoordinatorThread._generate_transaction_id())
                manager_socket.send_message(OpCode.START_TRANSACTION, [transaction_id_set[-1]], generator_socket)

            elif generator_message[0] == OpCode.INSERT_FROM_CLIENT:
                logger.info(f"Sending ACK back to generator.")
                manager_socket.send_response(ResponseCode.OK, generator_socket)

            elif generator_message[0] == OpCode.COMMIT_TRANSACTION:
                logger.info(f"Sending COMMIT back to generator.")
                manager_socket.send_response(ResponseCode.TRANSACTION_COMMITTED, generator_socket)

            elif generator_message[0] == OpCode.DISCONNECT_FROM_CLIENT:
                self.assertEqual(len(transaction_id_set), 3083)
                generator_socket.close()
                manager_socket.close()
                return

            else:
                logger.error(f"Unknown message sent from generator.")

    def test_abort_insert(self):
        manager_socket = communication.GenericSocketUser()
        manager_socket.socket.bind((socket.gethostname(), self.test_port + 1))
        manager_socket.socket.listen(5)

        # Spawn our generator thread.
        generator_thread = threading.Thread(target=self._generator_wrapper, args=(self.test_port + 1,))
        generator_thread.start()

        # Accept our generator connection.
        generator_socket, generator_address = manager_socket.socket.accept()
        logger.info(f"Connection accepted from {generator_address}.")

        transaction_id_set = []
        while True:
            generator_message = manager_socket.read_message(generator_socket)
            logger.info(f"Received message: {generator_message} from generator.")

            if generator_message[0] == OpCode.START_TRANSACTION:
                transaction_id_set.append(TransactionCoordinatorThread._generate_transaction_id())
                manager_socket.send_message(OpCode.START_TRANSACTION, [transaction_id_set[-1]], generator_socket)

            elif generator_message[0] == OpCode.INSERT_FROM_CLIENT:
                self.assertEqual(len(transaction_id_set), 1)
                logger.info(f"Sending ACK back to generator.")
                generator_socket.close()
                manager_socket.close()
                return

            else:
                logger.error(f"Unknown message sent from generator.")

    def test_abort_all(self):
        manager_socket = communication.GenericSocketUser()
        manager_socket.socket.bind((socket.gethostname(), self.test_port + 2))
        manager_socket.socket.listen(5)

        # Spawn our generator thread.
        generator_thread = threading.Thread(target=self._generator_wrapper, args=(self.test_port + 2,))
        generator_thread.start()

        # Accept our generator connection.
        generator_socket, generator_address = manager_socket.socket.accept()
        logger.info(f"Connection accepted from {generator_address}.")

        transaction_id_set = []
        while True:
            generator_message = manager_socket.read_message(generator_socket)
            logger.info(f"Received message: {generator_message} from generator.")

            if generator_message[0] == OpCode.START_TRANSACTION:
                transaction_id_set.append(TransactionCoordinatorThread._generate_transaction_id())
                manager_socket.send_message(OpCode.START_TRANSACTION, [transaction_id_set[-1]], generator_socket)

            elif generator_message[0] == OpCode.INSERT_FROM_CLIENT:
                logger.info(f"Sending ACK back to generator.")
                manager_socket.send_response(ResponseCode.OK, generator_socket)

            elif generator_message[0] == OpCode.COMMIT_TRANSACTION:
                logger.info(f"Sending ABORT back to generator.")
                manager_socket.send_response(ResponseCode.TRANSACTION_ABORTED, generator_socket)

            elif generator_message[0] == OpCode.DISCONNECT_FROM_CLIENT:
                self.assertEqual(len(transaction_id_set), 3083)
                generator_socket.close()
                manager_socket.close()
                return

            else:
                logger.error(f"Unknown message sent from generator.")
