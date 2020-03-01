""" This file contains all shared procedure / code / constants for this codebase. """
import socket
import logging
import pickle
import uuid
import sys

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

    # For recovery use when a TM fails.
    TRANSACTION_STATUS = 10
    RECONNECT_PARTICIPANT = 11


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
    logger = logging.getLogger(__qualname__)

    # The first portion of a message, the length, is of fixed size.
    MESSAGE_LENGTH_BYTE_SIZE = 5

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
                    self.close(client_socket)
                    return None
                else:
                    chunks.append(chunk)
                    bytes_read += len(chunk)

            # Obtain our length.
            message_length = pickle.loads(b''.join(chunks))
            self.logger.debug('Reading message of length: ', message_length)

            # Repeat for the message content.
            chunks, bytes_read = [], 0
            while bytes_read < message_length:
                chunk = working_socket.recv(min(message_length - bytes_read, 2048))

                if chunk == b'':
                    self.close(client_socket)
                    return None
                else:
                    chunks.append(chunk)
                    bytes_read += len(chunk)

            # Reconstruct message, and deserialize.
            received_message = pickle.loads(b''.join(chunks))
            self.logger.debug('Received message: ', received_message)
            return received_message

        except Exception:
            self.close(client_socket)
            return None

    def send_message(self, op_code: OpCode, contents: List, client_socket: socket.socket = None) -> bool:
        """ Correctly format a message to send to another socket user. """
        working_socket = self.socket if client_socket is None else client_socket
        message = [op_code] + contents

        serialized_message = pickle.dumps(message)
        try:
            working_socket.send(pickle.dumps(len(serialized_message)))
            working_socket.send(serialized_message)
            return True

        except Exception:
            self.close(client_socket)
            return False

    def send_op(self, op_code: OpCode, client_socket: socket.socket = None) -> bool:
        """ Correctly format a OP code to send to another socket user. """
        working_socket = self.socket if client_socket is None else client_socket
        message = [op_code]

        serialized_message = pickle.dumps(message)
        try:
            working_socket.send(pickle.dumps(len(serialized_message)))
            working_socket.send(serialized_message)
            return True

        except Exception:
            self.close(client_socket)
            return False

    def send_response(self, response_code: ResponseCode, client_socket: socket.socket = None) -> bool:
        """ Correctly format a response code to send to another socket user. """
        working_socket = self.socket if client_socket is None else client_socket
        message = [response_code]

        serialized_message = pickle.dumps(message)
        try:
            working_socket.send(pickle.dumps(len(serialized_message)))
            working_socket.send(serialized_message)
            return True

        except Exception:
            self.close(client_socket)
            return False

    def close(self, client_socket: socket.socket = None):
        if client_socket is not None:
            client_socket.close()
        self.socket.close()


class WriteAheadLogger(object):
    """ Class to standardize how WALs are formatted, written to, and read. """

    class Role(IntEnum):
        """ There exists two different types of WALs: coordinator and participant. """
        PARTICIPANT = 0
        COORDINATOR = 1

    @staticmethod
    def _transaction_id_to_bytes(transaction_id: str):
        return uuid.UUID(transaction_id).bytes

    @staticmethod
    def _transaction_id_to_str(transaction_id: bytes):
        return str(uuid.UUID(bytes=transaction_id))

    def _create_tables(self) -> None:
        """ Create the tables required for our log file, if they do not already exist. """
        pass

    def __init__(self, wal_file: str):
        """ Open our log file and create the tables if they do not exist. """
        pass

    def initialize_transaction(self, transaction_id: bytes, role: Role) -> None:
        """ Initialize a new transaction. We need to log our role in this transaction. """
        pass

    def get_role_in(self, transaction_id: bytes) -> Role:
        """ Return our role in the given transaction (either a coordinator or a participant). """
        pass

    def get_coordinator_for(self, transaction_id: bytes) -> int:
        """ Return the node-id of the coordinator associated with the given transaction. """
        pass

    def is_transaction_active(self, transaction_id: bytes) -> bool:
        """ Return true if the transaction has not been committed yet. False otherwise. """
        pass

    def add_participant(self, transaction_id: bytes, node_id: int) -> None:
        """ Add a new participant to the transaction. This operation should only be called by a coordinator. """
        pass

    def get_uncommitted_transactions(self) -> List[bytes]:
        """ Return a list of transactions that have not been committed yet. """
        pass

    def get_participants_in(self, transaction_id: bytes) -> List[int]:
        """ Return a list of node IDs that correspond to participants in the given transaction. """
        pass

    def log_statement(self, transaction_id: bytes, statement: str) -> None:
        """ Record the given statement with the given transaction. """
        pass

    def log_commit_of(self, transaction_id: bytes):
        """ Log that the given transaction has committed. """
        pass

    def log_abort_of(self, transaction_id: bytes):
        """ Log that the given transaction has been aborted. """
        pass

    def flush_log(self) -> None:
        """ Persist the log file to disk. """
        pass

    def get_redo_for(self, transaction_id: bytes) -> List[str]:
        """ Get a list of statements to redo a given transaction. """
        pass

    def get_undo_for(self, transaction_id: bytes) -> List[str]:
        """ Get a list of statements to undo a given transaction. """
        pass

    def close(self):
        """ Close the resource. """
        pass
