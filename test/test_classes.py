import threading
import unittest
import sys

from unittest import TestCase
from shared import *


class TestGenericSocketUser(TestCase):
    """ Verifies the class that allows processes to talk with one another. """

    logger = logging.getLogger(__qualname__)
    test_port = 55000

    class _TestServer(threading.Thread, GenericSocketUser):
        logger = logging.getLogger(__qualname__)

        def __init__(self, port: int):
            threading.Thread.__init__(self, daemon=True)
            GenericSocketUser.__init__(self)
            self.socket.bind((socket.gethostname(), port))
            self.socket.listen(5)

        def run(self) -> None:
            client_socket, client_address = self.socket.accept()
            self.logger.info(f"Connection accepted from {client_address}.")

            client_message = self.read_message(client_socket)
            self.logger.info(f"Message read from client: {client_message}.")

            self.send_message(client_message[0], client_message[1:], client_socket)
            self.logger.info("Sending same message back to client.")
            client_socket.close()

    def test_message_transfer(self):
        working_port = self.test_port
        server = self._TestServer(working_port)
        server.start()

        client = GenericSocketUser()
        client.socket.connect((socket.gethostname(), working_port))
        self.logger.info(f"Connected w/ server at {(socket.gethostname(), working_port)}.")
        client.send_message(OpCode.NO_OP, ['test'])
        self.logger.info(f"Sent test message.")

        response = client.read_message()
        self.logger.info(f"Response received.")
        self.assertEqual(response, [OpCode.NO_OP, 'test'])

        # Close our resources.
        server.join()
        client.socket.close()
        server.socket.close()

    def test_send_op(self):
        working_port = self.test_port + 1
        server = self._TestServer(working_port)
        server.start()

        client = GenericSocketUser()
        client.socket.connect((socket.gethostname(), working_port))
        self.logger.info(f"Connected w/ server at {(socket.gethostname(), working_port)}.")
        client.send_op(OpCode.NO_OP)
        self.logger.info(f"Sent test message.")

        response = client.read_message()
        self.logger.info(f"Response received.")
        self.assertEqual(response, [OpCode.NO_OP])

        # Close our resources.
        server.join()
        client.socket.close()
        server.socket.close()

    def test_send_response(self):
        working_port = self.test_port + 2
        server = self._TestServer(working_port)
        server.start()

        client = GenericSocketUser()
        client.socket.connect((socket.gethostname(), working_port))
        self.logger.info(f"Connected w/ server at {(socket.gethostname(), working_port)}.")
        client.send_response(ResponseCode.OK)
        self.logger.info(f"Sent test message.")

        response = client.read_message()
        self.logger.info(f"Response received.")
        self.assertEqual(response, [ResponseCode.OK])

        # Close our resources.
        server.join()
        client.socket.close()
        server.socket.close()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr)
    unittest.main()
