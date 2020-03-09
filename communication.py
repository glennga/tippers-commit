import socket
import logging
import pickle

from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class GenericSocketUser(object):
    """ Class to standardize message send and receipt. """
    # The first portion of a message, the length, is of fixed size. (2^8 maximum message length in bytes)
    MESSAGE_LENGTH_BYTE_SIZE = 8

    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def read_message(self, client_socket: socket.socket = None):
        """ Read message_length bytes from the specified socket, and deserialize our message. """
        working_socket = self.socket if client_socket is None else client_socket

        try:
            # Read our message length.
            chunks, bytes_read = [], 0
            while bytes_read < GenericSocketUser.MESSAGE_LENGTH_BYTE_SIZE:
                chunk = working_socket.recv(GenericSocketUser.MESSAGE_LENGTH_BYTE_SIZE - bytes_read)
                if chunk == b'':
                    raise EOFError("Working socket has been closed.")

                chunks.append(chunk)
                bytes_read += len(chunk)

            # Obtain our length.
            message_length = int.from_bytes(b''.join(chunks), byteorder='big')
            logger.debug(f'Reading message of length: {message_length}')

            # Repeat for the message content.
            chunks, bytes_read = [], 0
            while bytes_read < message_length:
                chunk = working_socket.recv(min(message_length - bytes_read, 2048))
                if chunk == b'':
                    raise EOFError("Working socket has been closed.")

                chunks.append(chunk)
                bytes_read += len(chunk)

            # Reconstruct message, and deserialize.
            received_message = pickle.loads(b''.join(chunks))
            logger.debug(f'Received message: {received_message}')
            return received_message

        except Exception as e:
            logger.warning(f"Exception caught: {e}")
            self.close(client_socket)
            return None

    def send_message(self, op_code: OpCode, contents: List, client_socket: socket.socket = None) -> bool:
        """ Correctly format a message to send to another socket user. """
        working_socket = self.socket if client_socket is None else client_socket
        message = [op_code] + contents

        serialized_message = pickle.dumps(message)
        message_length = len(serialized_message).to_bytes(GenericSocketUser.MESSAGE_LENGTH_BYTE_SIZE, byteorder='big')
        try:
            logger.debug(f"Sending message length: {len(serialized_message)} | {message_length}")
            working_socket.sendall(message_length)
            logger.debug(f"Sending message: {serialized_message}.")
            working_socket.sendall(serialized_message)
            return True

        except Exception as e:
            logger.warning(f"Exception caught: {e}")
            self.close(client_socket)
            return False

    def send_op(self, op_code: OpCode, client_socket: socket.socket = None) -> bool:
        """ Correctly format a OP code to send to another socket user. """
        working_socket = self.socket if client_socket is None else client_socket
        message = [op_code]

        serialized_message = pickle.dumps(message)
        message_length = len(serialized_message).to_bytes(GenericSocketUser.MESSAGE_LENGTH_BYTE_SIZE, byteorder='big')
        try:
            logger.debug(f"Sending message length: {len(serialized_message)} | {message_length}")
            working_socket.sendall(message_length)
            logger.debug(f"Sending message: {serialized_message}.")
            working_socket.sendall(serialized_message)
            return True

        except Exception as e:
            logger.warning(f"Exception caught: {e}")
            self.close(client_socket)
            return False

    def send_response(self, response_code: ResponseCode, client_socket: socket.socket = None) -> bool:
        """ Correctly format a response code to send to another socket user. """
        working_socket = self.socket if client_socket is None else client_socket
        message = [response_code]

        serialized_message = pickle.dumps(message)
        message_length = len(serialized_message).to_bytes(GenericSocketUser.MESSAGE_LENGTH_BYTE_SIZE, byteorder='big')
        try:
            logger.debug(f"Sending message length: {len(serialized_message)} | {message_length}")
            working_socket.sendall(message_length)
            logger.debug(f"Sending message: {serialized_message}.")
            working_socket.sendall(serialized_message)
            return True

        except Exception as e:
            logger.warning(f"Exception caught: {e}")
            self.close(client_socket)
            return False

    def close(self, client_socket: socket.socket = None):
        logger.info("'Close' called. Releasing socket(s).")
        if client_socket is not None:
            client_socket.close()
        self.socket.close()