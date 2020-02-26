""" This file contains all shared procedure / code / constants for this codebase. """
import socket
import logging
import pickle

from enum import IntEnum
from typing import List

# Size constants to be used when sending / receiving messages.
RESPONSE_CODE_BYTE_SIZE = 32
OP_CODE_BYTE_SIZE = 32
TRANSACTION_ID_BYTE_SIZE = 16
MESSAGE_LENGTH_BYTE_SIZE = 24


class OpCode(IntEnum):
    """ Operation codes. """
    STOP = -1
    NO_OP = 0

    # Between the coordinator and the client.
    START_TRANSACTION = 1
    ABORT_TRANSACTION = 2
    COMMIT_TRANSACTION = 3
    INSERT_FROM_CLIENT = 4

    # Between the coordinator and a participant.
    INITIATE_PARTICIPANT = 5
    INSERT_FROM_COORDINATOR = 6

    LOG = 1
    FLUSH_LOG = 2


class ResponseCode(IntEnum):
    """ Response codes. """
    OK = 0
    FAIL = 1


class GenericSocketUser(object):
    """ Class to standardize message send and receipt. """

    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def read_message(self, client_socket: socket.socket = None):
        """ Read message_length bytes from the specified socket, and deserialize our message. """
        working_socket = self.socket if client_socket is None else client_socket

        # Read our message length.
        chunks, bytes_read = [], 0
        while bytes_read < MESSAGE_LENGTH_BYTE_SIZE:
            chunk = working_socket.recv(MESSAGE_LENGTH_BYTE_SIZE - bytes_read)

            if chunk == b'':
                exception_message = "Socket connection broken."
                logging.error(exception_message)
                raise RuntimeError(exception_message)
            else:
                chunks.append(chunk)
                bytes_read += len(chunk)

        # Obtain our length.
        message_length = int.from_bytes(b''.join(chunks), byteorder='big')
        logging.debug('Reading message of length: ', message_length)

        # Repeat for the message content.
        chunks, bytes_read = [], 0
        while bytes_read < message_length:
            chunk = working_socket.recv(min(message_length - bytes_read, 2048))

            if chunk == b'':
                exception_message = "Socket connection broken."
                logging.error(exception_message)
                raise RuntimeError(exception_message)
            else:
                chunks.append(chunk)
                bytes_read += len(chunk)

        # Reconstruct message, and deserialize.
        received_message = pickle.loads(b''.join(chunks))
        logging.debug('Received message: ', received_message)
        return received_message

    def send_message(self, op_code: OpCode, contents: List, client_socket: socket.socket = None):
        """ Correctly format a message to send to another socket user. """
        working_socket = self.socket if client_socket is None else client_socket
        message = [op_code] + contents

        # The first item we always send is the message length.
        working_socket.send(bytes(len(message)))

        # Next, the message itself.
        working_socket.send(pickle.loads(message))

    def send_op(self, op_code: OpCode, client_socket: socket.socket = None):
        """ Correctly format a OP code to send to another socket user. """
        working_socket = self.socket if client_socket is None else client_socket
        message = [op_code]

        # The first item we always send is the message length.
        working_socket.send(bytes(len(message)))

        # Next, the message itself.
        working_socket.send(pickle.loads(message))

    def send_response(self, response_code: ResponseCode, client_socket: socket.socket = None):
        """ Correctly format a response code to send to another socket user. """
        working_socket = self.socket if client_socket is None else client_socket
        message = [response_code]

        # The first item we always send is the message length.
        working_socket.send(bytes(len(message)))

        # Next, the message itself.
        working_socket.send(pickle.loads(message))
