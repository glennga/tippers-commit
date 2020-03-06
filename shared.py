""" This file contains all shared procedure / code / constants for this codebase. """
import socket
import logging
import pickle
import uuid
import sqlite3


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

    # For recovery use when a TM fails.
    TRANSACTION_COMMITTED = 5
    TRANSACTION_ABORTED = 6


class TransactionRole(IntEnum):
    """ There exists two different types of roles: coordinator and participant. """
    PARTICIPANT = 0
    COORDINATOR = 1


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
        self.logger.info("'Close' called. Releasing socket(s).")
        if client_socket is not None:
            client_socket.close()
        self.socket.close()


class WriteAheadLogger(object):
    """ Class to standardize how WALs are formatted, written to, and read. """

    @staticmethod
    def _transaction_id_to_bytes(transaction_id: str):
        return uuid.UUID(transaction_id).bytes

    @staticmethod
    def _transaction_id_to_str(transaction_id: bytes):
        return str(uuid.UUID(bytes=transaction_id))


    ############# TEMP SCHEMA WILL MODIFY LATER ######################    
    def _create_tables(self) -> None:
        """ Create the tables required for our log file, if they do not already exist. """


        table_1 = "CREATE TABLE IF NOT EXISTS PROTOCOL_DB (tr_id varchar(255) PRIMARY KEY, location int)"
        table_2 = "CREATE TABLE IF NOT EXISTS QUERY_DB (tr_id varchar(255), statement text)"
        table_3 = "CREATE TABLE IF NOT EXISTS COORDINATOR_DB (tr_id varchar(255) PRIMARY KEY , coordinator int, coord_flag int)"
        table_4 = "CREATE TABLE IF NOT EXISTS STATE_TABLE (tr_id varchar(255), STATE VARCHAR(100))"

        cur = self.conn.cursor()
        
        cur.execute(table_1)
        cur.execute(table_2)
        cur.execute(table_3)

        
        
        
        

    def __init__(self, wal_file: str):
        """ Open our log file and create the tables if they do not exist. """
        self.conn = sqlite3.connect(wal_file)

        self._create_tables()

        
   
    def initialize_transaction(self, transaction_id: bytes, role: TransactionRole, node_id: int) -> None:
        """ Initialize a new transaction. We need to log our role in this transaction. """

        tr_id = self._transaction_id_to_str(transaction_id)

        query = "INSERT INTO STATE_TABLE VALUES('" + tr_id + "', I)"

        cur = self.conn.cursor()
        cur.execute(query)

        
        pass


   
    def get_role_in(self, transaction_id: bytes) -> TransactionRole:
        """ Return our role in the given transaction (either a coordinator or a participant). """
        tr_id = self._transaction_id_to_str(transaction_id)

        cur = self.conn.cursor()

        query = "SELECT FROM COORDINATOR_DB WHERE tr_id = '" + tr_id + "'"
        cur.execute(query)

        result_set = cur.fetchone()

        if len(result_set) <= 0:
            return -1

        else:
            if result_set[2] == 0:
                return TransactionRole.PARTICIPANT

            else:
                return TransactionRole.COORDINATOR


  
    def get_coordinator_for(self, transaction_id: bytes) -> int:
        """ Return the node-id of the coordinator associated with the given transaction. """

        tr_id = self._transaction_id_to_str(transaction_id)
        query = "SELECT coordinator FROM COORDINATOR_DB WHERE tr_id = '" + tr_id + "'"

        cur = self.conn.cursor()

        cur.execute(query)
        result_set = cur.fetchone()

        if len(result_set < 1):
            return -1

        else:
            return result_set
        

    def is_transaction_prepared(self, transaction_id: bytes) -> bool:
        """ Return true if we (a participant) have prepared to commit. False otherwise. """

        tr_id = self._transaction_id_to_str(transaction_id)

        query = "SELECT state FROM STATE_TABLE WHERE tr_id = '" + tr_id + "'"

        cur = self.conn.cursor()
        cur.execute(query)

        result_set = cur.fetchone()
        if len(result_set) < 1:
            return False

        else:
            if result_set[0] = "P":
                return True


        return False

    def add_participant(self, transaction_id: bytes, node_id: int) -> None:
        """ Add a new participant to the transaction. This operation should only be called by a coordinator. """

        tr_id = self._transaction_id_to_str(transaction_id)

        query = "INSERT INTO STATE_TABLE VALUES('" + tr_id + "', 'I') "

        cur = self.conn.cursor()
        cur.execute(query)



    
        
    ## also setting transactino to initiated in statetable
    def add_coordinator(self, transaction_id: bytes, node_id: int, self_flag: int) -> None:
        """ Add coordinator to coordinator db"""
        tr_id = self._transaction_id_to_str(transaction_id)

        query1 = "INSERT INTO COORDINATOR_DB VALUES('" + tr_id + "', '" + str(node_id) + "', '" + str(self_flag) +"')"
        query2 = "INSERT INTO STATE_TABLE VALUES ('" + tr_id  + "', 'I')"

        cur = self.conn.cursor()
        cur.execute(query)



        
    
    def get_uncommitted_transactions(self) -> List[bytes]:
        """ Return a list of transactions that have not been committed yet. """
        #Also returning transactionst that are not done

        tr_id = self._transaction_id_to_str(transaction_id)

        query = "SELECT tr_id FROM STATE_TABLE WHERE state = 'I' or state = 'P'"

        cur = self.conn.cursor()
        cur.execute(query)

        result_set = query.fetchall()

        return [self._transaction_id_to_bytes(i) for i in result_set]

    


    def prepare_transaction(self, transaction_id: bytes):
        tr_id = self._transaction_id_to_str(transaction_id)

        query = "INSERT INTO PROTOCOL_DB VALUES('" + tr_id + "', 'P')" 

        cur = self.conn.cursor()
        cur.execute(query)
        



    def get_participants_in(self, transaction_id: bytes) -> List[int]:
        """ Return a list of node IDs that correspond to participants in the given transaction. """
        tr_id = self._transaction_id_to_str(transaction_id)

        query = "SELECT location from PROTOCOL_DB WHERE tr_id = '" + tr_id + "'"

        cur = self.conn.cursor()
        cur.execute(query)

        result_set = cur.fetchall()
        
        return list(result_set)




        
    def log_statement(self, transaction_id: bytes, statement: str) -> None:
        """ Record the given statement with the given transaction. """

        tr_id = self._transaction_id_to_str(transaction_id)
        query = "INSERT INTO QUERY_DB (tr_id, statement) VALUES ('" + tr_id + "', '" + statement + "')"

        cur = self.conn.cursor()
        cur.execute(cur)




    def log_commit_of(self, transaction_id: bytes):
        """ Log that the given transaction has committed. """
        ## Update state table to commit (If coordinator)
        tr_id = self._transaction_id_to_str(transaction_id)

        query = "INSERT INTO PROTOCOL_DB VALUES('" + tr_id + "', 'C')" 

        cur = self.conn.cursor()
        cur.execute(query)
        
        
        

    def log_abort_of(self, transaction_id: bytes):
        """ Log that the given transaction has been aborted. """
        ## Update state table to abort (If coordinator)
        tr_id = self._transaction_id_to_str(transaction_id)

        query = "INSERT INTO PROTOCOL_DB VALUES('" + tr_id + "', 'A')" 

        cur = self.conn.cursor()
        cur.execute(query)
        


    def log_completion_of(self, transaction_id: bytes):
        #UPDATE STATE TABLE TO D
        tr_id = self._transaction_id_to_str(transaction_id)

        query = "INSERT INTO PROTOCOL_DB VALUES('" + tr_id + "', 'D')" 

        cur = self.conn.cursor()
        cur.execute(query)
        
    

    def flush_log(self) -> None:
        """ Persist the log file to disk. """
        self.conn.commit()

    def get_redo_for(self, transaction_id: bytes) -> List[str]:
        """ Get a list of statements to redo a given transaction. """
        tr_id = self._transaction_id_to_str(transaction_id)

        query = "SELECT statement from QUERY_DB WHERE tr_id = '" + tr_id + "' ORDER BY order;"

        cur = self.conn.cursor()
        cur.execute(query)

        result_set = [str(i) fo i in list(cur.fetchall())]

        return result_set



    def _get_table_name(self, statement):
        statement_split_by_into = statement.split("into")
        statement_split_by_values = statement_split_by_into[1].split("values")

        table_name = statement_split_by_values[0]
        table_name = table_name.replace(' ', "")

        return table_name


    def _get_insert_id(self, statement):
        #Split by (
        #split by ,
        #output res[0]
        first_split = statement.split("(")
        second_split = first_split[1].split(",")

        return second_split[0]

    

    ### Make sure all ddl files are sorted lowercase, SQL SAFE UPDATES = 0######    
    def _get_inverse_statement(self, statement):
        table_name = self._get_table_name(statement)

        insert_id = self._get_insert_id(statement)
        
        delte_query = "DELETE FROM " + table_name + "WHERE id = " + insert_id

        return delete_query
        


    def get_undo_for(self, transaction_id: bytes) -> List[str]:
        """ Get a list of statements to undo a given transaction. """

        statements = self.get_redo_for(self, transaction_id)[::-1] ## Undo in reverse chronological order

        undo_statements = []
        for statement in statements:
            undo_statement.append(self._get_inverse_statement, statement)

        return undo_statements

    def close(self):
        """ Close the resource. """
        self.flush_log()
        self.conn.close()
