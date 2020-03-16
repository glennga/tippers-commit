""" This file holds all protocol-DB related functionality. """
import sqlite3
import logging

from typing import List
from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class ProtocolDatabase(object):
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
        cur.execute(""" -- This is append-only. --
            CREATE TABLE IF NOT EXISTS STATE_LOG (
                tr_id TEXT, 
                state TEXT
            );
        """)

    def __init__(self, database_file: str):
        self.conn = sqlite3.connect(database_file, check_same_thread=False)
        self._create_tables()

    def log_initialize_of(self, transaction_id: str, role: TransactionRole) -> None:
        logger.info(f"Transaction {transaction_id} has been initialized with role {role}.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO STATE_LOG (tr_id, state)
            VALUES (?, "I");
        """, (transaction_id,))
        cur.execute("""
            INSERT INTO TRANSACTION_LOG (tr_id, tr_role)
            VALUES (?, ?);
        """, (transaction_id, 0 if role == TransactionRole.PARTICIPANT else 1,))
        self.conn.commit()

    def add_participant(self, transaction_id: str, node_id: int) -> None:
        logger.info(f"Adding participant {node_id} to transaction {transaction_id}.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO TRANSACTION_SITE_LOG (tr_id, tr_role, node_id)
            VALUES (?, 0, ?);
        """, (transaction_id, node_id,))
        self.conn.commit()

    def add_coordinator(self, transaction_id: str, node_id: int) -> None:
        logger.info(f"Adding coordinator {node_id} to transaction {transaction_id}.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO TRANSACTION_SITE_LOG (tr_id, tr_role, node_id)
            VALUES (?, 1, ?);
        """, (transaction_id, node_id,))
        self.conn.commit()

    def get_abortable_transactions(self) -> List[str]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT tr_id, GROUP_CONCAT(state)
            FROM STATE_LOG
            GROUP BY tr_id 
            HAVING GROUP_CONCAT(state) NOT LIKE "%C" AND 
                   GROUP_CONCAT(state) NOT LIKE "%P" AND 
                   GROUP_CONCAT(state) NOT LIKE "%A";
        """)
        return [i[0] for i in cur.fetchall()]

    def get_prepared_transactions(self) -> List[str]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT tr_id, GROUP_CONCAT(state)
            FROM STATE_LOG
            GROUP BY tr_id 
            HAVING GROUP_CONCAT(state) NOT LIKE "%C" AND 
                   GROUP_CONCAT(state) NOT LIKE "%I" AND 
                   GROUP_CONCAT(state) NOT LIKE "%A";
        """)
        return [i[0] for i in cur.fetchall()]

    def get_role_in(self, transaction_id: str) -> TransactionRole:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT tr_role
            FROM TRANSACTION_LOG
            WHERE tr_id = ?;
        """, (transaction_id,))

        result_set = cur.fetchone()
        if len(result_set) <= 0:
            logger.fatal(f"Error: Transaction {transaction_id} does not exist in the protocol database.")
            raise SystemError(f"Error: Transaction {transaction_id} does not exist in the protocol database.")

        elif result_set[0] == 0:
            logger.info(f"Role in transaction {transaction_id} is participant.")
            return TransactionRole.PARTICIPANT
        else:
            logger.info(f"Role in transaction {transaction_id} is coordinator.")
            return TransactionRole.COORDINATOR

    def get_participants_in(self, transaction_id: str) -> List[int]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT node_id
            FROM TRANSACTION_SITE_LOG
            WHERE tr_id = ? AND tr_role = 0;
        """, (transaction_id,))
        return [i[0] for i in cur.fetchall()]

    def get_coordinator_for(self, transaction_id: str) -> int:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT node_id
            FROM TRANSACTION_SITE_LOG
            WHERE tr_id = ? AND tr_role = 1;
        """, (transaction_id,))

        result_set = cur.fetchall()
        if len(result_set) < 1:
            logger.fatal(f"Error: Transaction {transaction_id} does not exist in the protocol database.")
            raise SystemError(f"Error: Transaction {transaction_id} does not exist in the database.")

        return result_set[0][0]

    def log_prepare_of(self, transaction_id: str):
        logger.info(f"Transaction {transaction_id} has been prepared.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO STATE_LOG (tr_id, state)
            VALUES (?, "P");
        """, (transaction_id,))
        self.conn.commit()

    def log_commit_of(self, transaction_id: str):
        logger.info(f"Transaction {transaction_id} has been committed.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO STATE_LOG (tr_id, state)
            VALUES (?, "C");
        """, (transaction_id,))
        self.conn.commit()

    def log_abort_of(self, transaction_id: str):
        logger.info(f"Transaction {transaction_id} has been aborted.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO STATE_LOG (tr_id, state)
            VALUES (?, "A");
        """, (transaction_id,))
        self.conn.commit()

    def log_completion_of(self, transaction_id: str):
        logger.info(f"Transaction {transaction_id} has been completed.")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO STATE_LOG (tr_id, state)
            VALUES (?, "D");
        """, (transaction_id,))
        self.conn.commit()

    def close(self):
        self.conn.commit()
        self.conn.close()
