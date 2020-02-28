""" This file contains all shared procedure / code / constants for this codebase. """
import socket
import logging
import pickle
import sys
import os
import uuid

from enum import IntEnum
from typing import List


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
    PREPARE_TO_COMMIT = 7
    COMMIT_FROM_COORDINATOR = 8
    ROLLBACK_FROM_COORDINATOR = 9


class ResponseCode(IntEnum):
    """ Response codes. """
    OK = 0
    FAIL = 1

    # Two-phase commit codes: only between a coordinator and a participant.
    PREPARED_FROM_PARTICIPANT = 2
    ABORT_FROM_PARTICIPANT = 3
    ACKNOWLEDGE_END = 4


class GenericSocketUser(object):
    """ Class to standardize message send and receipt. """

    # The first portion of a message, the length, is of fixed size.
    MESSAGE_LENGTH_BYTE_SIZE = 28

    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def read_message(self, client_socket: socket.socket = None):
        """ Read message_length bytes from the specified socket, and deserialize our message. """
        working_socket = self.socket if client_socket is None else client_socket

        # Read our message length.
        chunks, bytes_read = [], 0
        while bytes_read < GenericSocketUser.MESSAGE_LENGTH_BYTE_SIZE:
            chunk = working_socket.recv(GenericSocketUser.MESSAGE_LENGTH_BYTE_SIZE - bytes_read)

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
        working_socket.send(bytes(sys.getsizeof(message)))

        # Next, the message itself.
        working_socket.send(pickle.loads(message))

    def send_op(self, op_code: OpCode, client_socket: socket.socket = None):
        """ Correctly format a OP code to send to another socket user. """
        working_socket = self.socket if client_socket is None else client_socket
        message = [op_code]

        # The first item we always send is the message length.
        working_socket.send(bytes(sys.getsizeof(message)))

        # Next, the message itself.
        working_socket.send(pickle.loads(message))

    def send_response(self, response_code: ResponseCode, client_socket: socket.socket = None):
        """ Correctly format a response code to send to another socket user. """
        working_socket = self.socket if client_socket is None else client_socket
        message = [response_code]

        # The first item we always send is the message length.
        working_socket.send(bytes(sys.getsizeof(message)))

        # Next, the message itself.
        working_socket.send(pickle.loads(message))


class WriteAheadLogger(object):
    """ Class to standardize how WALs are formatted, written to, and read. """

    @staticmethod
    def parse_transaction_id(filename: str) -> bytes:
        """ The transaction ID is encoded in the filename. """
        return uuid.UUID(filename.split('.')[1]).bytes

    def __init__(self, transaction_id: bytes, wal_prefix: str):
        self.log_file = open(wal_prefix + '.' + str(uuid.UUID(bytes=transaction_id)), 'rw')
        self.transaction_id = transaction_id

        # We keep a cursor for iterating through our log file.
        self.cursor_position = self.log_file.tell()

    def create_log(self, site_size: int = 0):
        """ Create a new instance of a log file. """
        self.log_file.seek(0)  # The header determines our role and the participants of our transactions.
        self.log_file.write((('C' + ''.join('0' for _ in range(site_size))) if self.is_coordinator else 'P') + '\n')
        self.cursor_position = self.log_file.tell()

    def add_participant(self, node_id: int):
        if not self.is_coordinator:
            error_message = 'Participants may not add other participants.'
            logging.error(error_message)
            raise PermissionError(error_message)

        # Set the "bit" vector that denotes our active participants.
        self.log_file.seek(1 + node_id)
        self.log_file.write('1')

    def get_participant_ids(self, site_size: int):
        if not self.is_coordinator:
            error_message = 'Participants should be site-unaware.'
            logging.error(error_message)
            raise PermissionError(error_message)

        # Participants are denoted by a 1 in our "bit" vector.
        participant_ids = []
        self.log_file.seek(1)
        for i in range(site_size):
            if self.log_file.read(1) == '1':
                participant_ids.append(i)

        return participant_ids

    def log_statement(self, statement: str):
        """ Log the SQL statement to the end of our file. Super naive implementation!! :-( """
        self.set_cursor_end()
        self.log_file.write(statement)

    def flush_log(self):
        self.log_file.flush()

    def set_cursor_start(self):
        self.log_file.seek(0)
        self.cursor_position = self.log_file.tell()

    def set_cursor_end(self):
        self.log_file.seek(0, os.SEEK_END)
        self.cursor_position = self.log_file.tell()

    def get_next_entry(self) -> str:
        return self.log_file.readline()

    def get_prev_entry(self) -> str:
        line = ''
        while self.cursor_position >= 0:
            self.log_file.seek(self.cursor_position)
            next_char = self.log_file.read(1)

            # Read until we reach the previous line.
            if next_char == '\n':
                return line[::-1]

            else:
                line += next_char
            self.cursor_position -= 1

        return line[::-1]
