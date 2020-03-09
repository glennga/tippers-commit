import uuid
import sqlite3
import logging

from typing import List, Tuple
from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class WriteAheadLogger(object):
    """ Class to standardize how WALs are formatted, written to, and read. """

    @staticmethod
    def _transaction_id_to_bytes(transaction_id: str):
        return uuid.UUID(transaction_id).bytes

    @staticmethod
    def _transaction_id_to_str(transaction_id: bytes):
        return str(uuid.UUID(bytes=transaction_id))

    def _create_tables(self) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS TRANSACTION_LOG (
                tr_id TEXT PRIMARY KEY,
                tr_role INT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS TRANSACTION_SITE_LOG (
                tr_id TEXT,
                tr_role INT,
                node_id INT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ACTION_LOG (
                tr_id TEXT, 
                logged_action TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS STATE_LOG (
                tr_id TEXT, 
                state TEXT
            );
        """)

    def __init__(self, wal_file: str):
        self.conn = sqlite3.connect(wal_file)
        self._create_tables()

    def initialize_transaction(self, transaction_id: bytes, role: TransactionRole) -> None:
        tr_id = self._transaction_id_to_str(transaction_id)
        logger.info(f"Initializing transaction {transaction_id} with role {role}.")

        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO STATE_LOG (tr_id, state)
            VALUES (?, "I");
        """, (tr_id,))
        cur.execute("""
            INSERT INTO TRANSACTION_LOG (tr_id, tr_role)
            VALUES (?, ?);
        """, (tr_id, 0 if role == TransactionRole.PARTICIPANT else 1,))

    def add_participant(self, transaction_id: bytes, node_id: int) -> None:
        logger.info(f"Adding participant {node_id} to transaction {transaction_id}.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO TRANSACTION_SITE_LOG (tr_id, tr_role, node_id)
            VALUES (?, 0, ?);
        """, (self._transaction_id_to_str(transaction_id), node_id,))

    def add_coordinator(self, transaction_id: bytes, node_id: int) -> None:
        logger.info(f"Adding coordinator {node_id} to transaction {transaction_id}.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO TRANSACTION_SITE_LOG (tr_id, tr_role, node_id)
            VALUES (?, 1, ?);
        """, (self._transaction_id_to_str(transaction_id), node_id,))

    def get_role_in(self, transaction_id: bytes) -> TransactionRole:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT tr_role
            FROM TRANSACTION_LOG
            WHERE tr_id = ?;
        """, (self._transaction_id_to_str(transaction_id),))

        result_set = cur.fetchone()
        if len(result_set) <= 0:
            logger.fatal(f"Error: Transaction {transaction_id} does not exist in the log.")
            raise SystemError(f"Error: Transaction {transaction_id} does not exist in the log.")

        elif result_set[0] == 0:
            logger.info(f"Role in transaction {transaction_id} is participant.")
            return TransactionRole.PARTICIPANT
        else:
            logger.info(f"Role in transaction {transaction_id} is coordinator.")
            return TransactionRole.COORDINATOR

    def get_participants_in(self, transaction_id: bytes) -> List[int]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT node_id
            FROM TRANSACTION_SITE_LOG
            WHERE tr_id = ?;
        """, (self._transaction_id_to_str(transaction_id),))
        return [i[0] for i in cur.fetchall()]

    def get_coordinator_for(self, transaction_id: bytes) -> int:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT node_id
            FROM TRANSACTION_SITE_LOG
            WHERE tr_id = ? AND tr_role = 1;
        """, (self._transaction_id_to_str(transaction_id),))

        result_set = cur.fetchone()
        if len(result_set < 1):
            logger.fatal(f"Error: Transaction {transaction_id} does not exist in the log.")
            raise SystemError(f"Error: Transaction {transaction_id} does not exist in the log.")

        return result_set[0]

    def get_uncommitted_transactions(self) -> List[Tuple[bytes, str]]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT tr_id, GROUP_CONCAT(state)
            FROM STATE_LOG
            GROUP BY tr_id
            HAVING GROUP_CONCAT(state) NOT LIKE "%C" AND
                   GROUP_CONCAT(state) NOT LIKE "%A";
        """)

        get_state_of = lambda a: 'P' if 'P' in a else 'I'
        return [(self._transaction_id_to_bytes(i[0]), get_state_of(i[1]),) for i in cur.fetchall()]

    def prepare_transaction(self, transaction_id: bytes):
        logger.info(f"We have been asked to prepare transaction {transaction_id}.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO STATE_LOG (tr_id, state)
            VALUES (?, "P");
        """, (self._transaction_id_to_str(transaction_id),))

    def log_statement(self, transaction_id: bytes, statement: str) -> None:
        statement_no_newlines = statement.replace('\n', '').replace(' ', '')
        logger.debug(f"Logging '{statement_no_newlines}' for transaction {transaction_id}.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO ACTION_LOG (tr_id, logged_action)
            VALUES (?, ?);
        """, (self._transaction_id_to_str(transaction_id), statement,))

    def log_commit_of(self, transaction_id: bytes):
        logger.info(f"We have been asked to commit transaction {transaction_id}.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO STATE_LOG (tr_id, state)
            VALUES (?, "C");
        """, (self._transaction_id_to_str(transaction_id),))

    def log_abort_of(self, transaction_id: bytes):
        logger.info(f"We have been asked to abort transaction {transaction_id}.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO STATE_LOG (tr_id, state)
            VALUES (?, "A");
        """, (self._transaction_id_to_str(transaction_id),))

    def log_completion_of(self, transaction_id: bytes):
        logger.info(f"Logging completion of transaction {transaction_id}.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO STATE_LOG (tr_id, state)
            VALUES (?, "D");
        """, (self._transaction_id_to_str(transaction_id),))

    def flush_log(self) -> None:
        self.conn.commit()

    def get_redo_for(self, transaction_id: bytes) -> List[str]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT logged_action
            FROM ACTION_LOG
            WHERE tr_id = ?
            ORDER BY rowid;
        """, (self._transaction_id_to_str(transaction_id),))
        return [str(i[0]) for i in list(cur.fetchall())]

    @staticmethod
    def _get_table_name(statement):
        statement_split_by_into = statement.split("into")
        statement_split_by_values = statement_split_by_into[1].split("values")
        table_name = statement_split_by_values[0]
        table_name = table_name.replace(' ', "")
        return table_name

    @staticmethod
    def _get_insert_id(statement):
        first_split = statement.split("(")
        second_split = first_split[1].split(",")
        return second_split[0]

    def _get_inverse_statement(self, statement):
        table_name = self._get_table_name(statement)
        insert_id = self._get_insert_id(statement)
        return "DELETE FROM " + table_name + "WHERE id = " + insert_id + ";"

    def get_undo_for(self, transaction_id: bytes) -> List[str]:
        statements = self.get_redo_for(transaction_id)[::-1]  # Undo in reverse chronological order.
        return list(map(self._get_inverse_statement, statements))

    def close(self):
        self.flush_log()
        self.conn.close()
