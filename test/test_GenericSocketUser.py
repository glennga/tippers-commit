import threading
import unittest
import logging
import socket
import random
import string

from communication import GenericSocketUser
from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class TestGenericSocketUser(unittest.TestCase):
    """ Verifies the class that allows processes to talk with one another. """
    test_port = 50000

    class _TestServer(threading.Thread, GenericSocketUser):
        logger = logging.getLogger(__qualname__)

        def __init__(self, port: int):
            threading.Thread.__init__(self, daemon=True)
            GenericSocketUser.__init__(self)
            self.socket.bind((socket.gethostname(), port))
            self.socket.listen(5)

        def run(self) -> None:
            client_socket, client_address = self.socket.accept()
            logger.info(f"Connection accepted from {client_address}.")

            client_message = self.read_message(client_socket)
            logger.info(f"Message read from client: {client_message}.")

            self.send_message(client_message[0], client_message[1:], client_socket)
            logger.info("Sending same message back to client.")
            client_socket.close()

    class _TestFaultyServer(threading.Thread, GenericSocketUser):
        logger = logging.getLogger(__qualname__)

        def __init__(self, port: int):
            threading.Thread.__init__(self, daemon=True)
            GenericSocketUser.__init__(self)
            self.socket.bind((socket.gethostname(), port))
            self.socket.listen(5)

        def run(self) -> None:
            client_socket, client_address = self.socket.accept()
            logger.info(f"Connection accepted from {client_address}.")
            self.close(client_socket)

    def test_message_transfer(self):
        working_port = self.test_port
        server = self._TestServer(working_port)
        server.start()

        client = GenericSocketUser()
        client.socket.connect((socket.gethostname(), working_port))
        logger.info(f"Connected w/ server at {(socket.gethostname(), working_port)}.")
        status = client.send_message(OpCode.NO_OP, ['test'])
        logger.info(f"Sent test message.")
        self.assertTrue(status)

        response = client.read_message()
        logger.info(f"Response received.")
        self.assertEqual(response, [OpCode.NO_OP, 'test'])

        # Close our resources.
        server.join()
        client.socket.close()
        server.socket.close()

    def test_large_message_transfer(self):
        working_port = self.test_port + 1
        server = self._TestServer(working_port)
        server.start()

        large_message = ''.join(random.choices(string.ascii_uppercase + string.digits, k=300))
        client = GenericSocketUser()
        client.socket.connect((socket.gethostname(), working_port))
        logger.info(f"Connected w/ server at {(socket.gethostname(), working_port)}.")
        status = client.send_message(OpCode.NO_OP, [large_message])
        logger.info(f"Sent test message.")
        self.assertTrue(status)

        response = client.read_message()
        logger.info(f"Response received.")
        self.assertEqual(response, [OpCode.NO_OP, large_message])

        # Close our resources.
        server.join()
        client.socket.close()
        server.socket.close()

    def test_op_transfer(self):
        working_port = self.test_port + 2
        server = self._TestServer(working_port)
        server.start()

        client = GenericSocketUser()
        client.socket.connect((socket.gethostname(), working_port))
        logger.info(f"Connected w/ server at {(socket.gethostname(), working_port)}.")
        client.send_op(OpCode.NO_OP)
        logger.info(f"Sent test message.")

        response = client.read_message()
        logger.info(f"Response received.")
        self.assertEqual(response, [OpCode.NO_OP])

        # Close our resources.
        server.join()
        client.socket.close()
        server.socket.close()

    def test_response_transfer(self):
        working_port = self.test_port + 3
        server = self._TestServer(working_port)
        server.start()

        client = GenericSocketUser()
        client.socket.connect((socket.gethostname(), working_port))
        logger.info(f"Connected w/ server at {(socket.gethostname(), working_port)}.")
        client.send_response(ResponseCode.OK)
        logger.info(f"Sent test message.")

        response = client.read_message()
        logger.info(f"Response received.")
        self.assertEqual(response, [ResponseCode.OK])

        # Close our resources.
        server.join()
        client.socket.close()
        server.socket.close()

    def test_faulty_receive(self):
        working_port = self.test_port + 4
        server = self._TestFaultyServer(working_port)
        server.start()

        client = GenericSocketUser()
        client.socket.connect((socket.gethostname(), working_port))
        logger.info(f"Connected w/ server at {(socket.gethostname(), working_port)}.")
        response = client.read_message()
        self.assertIsNone(response)

        # Close our resources.
        server.join()
        client.socket.close()
        server.socket.close()

    def test_faulty_send(self):
        working_port = self.test_port + 5
        server = self._TestFaultyServer(working_port)
        server.start()

        client = GenericSocketUser()
        client.socket.connect((socket.gethostname(), working_port))
        logger.info(f"Connected w/ server at {(socket.gethostname(), working_port)}.")

        status = client.send_response(ResponseCode.OK)
        logger.info(f"Sent test message.")
        self.assertFalse(status)

        # Close our resources.
        server.join()
        client.socket.close()
        server.socket.close()
