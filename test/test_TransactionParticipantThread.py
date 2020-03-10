import communication
import participate
import psycopg2
import unittest
import logging
import socket
import time
import json
import uuid
import os

from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class TestTransactionParticipantThread(unittest.TestCase):
    test_file = 'test_database.log'
    test_port = 49000

    @staticmethod
    def get_postgres_connection():
        with open('config/postgres.json') as postgres_config_file:
            return psycopg2.connect(**json.load(postgres_config_file))

    @staticmethod
    def get_postgres_context():
        with open('config/postgres.json') as postgres_config_file:
            postgres_json = json.load(postgres_config_file)
            return {
                "postgres_username": postgres_json['user'],
                "postgres_password": postgres_json['password'],
                "postgres_hostname": postgres_json['host'],
                "postgres_database": postgres_json['database']
            }

    @classmethod
    def setUpClass(cls) -> None:
        try:
            os.remove(cls.test_file)
            os.remove(cls.test_file + '-journal')

        except OSError:
            pass

        try:
            conn = cls.get_postgres_connection()
            cur = conn.cursor()
            cur.execute("""
                DELETE FROM thermometerobservation
                WHERE TRUE;
            """)
            conn.commit()
            conn.close()

        except Exception:
            pass

    def tearDown(self) -> None:
        self.setUpClass()

    def test_single_commit(self):
        participant_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        participant_socket.bind((socket.gethostname(), self.test_port))
        participant_socket.listen(5)

        # Connect to our coordinator.
        coordinator_socket = communication.GenericSocketUser()
        coordinator_socket.socket.connect((socket.gethostname(), self.test_port,))
        from_coordinator_socket, from_coordinator_address = participant_socket.accept()
        participant_thread = participate.TransactionParticipantThread(
            **self.get_postgres_context(),
            transaction_id=str(uuid.uuid4()),
            client_socket=from_coordinator_socket,
            protocol_db=self.test_file,
            failure_time=5,
        )
        participant_thread.start()
        time.sleep(0.01)

        coordinator_socket.send_message(OpCode.INSERT_FROM_COORDINATOR, ["""
            INSERT INTO thermometerobservation 
            VALUES ('a239a033-b340-426d-a686-ad32908709ae', 48, '2017-11-08 00:00:00', 
                    '9592a785_d3a4_4de2_bc3d_cfa1a127bf40');
        """])
        self.assertEqual(coordinator_socket.read_message(), [ResponseCode.OK])

        coordinator_socket.send_op(OpCode.PREPARE_TO_COMMIT)
        self.assertEqual(coordinator_socket.read_message(), [ResponseCode.PREPARED_FROM_PARTICIPANT])
        coordinator_socket.send_op(OpCode.COMMIT_FROM_COORDINATOR)
        self.assertEqual(coordinator_socket.read_message(), [ResponseCode.ACKNOWLEDGE_END])
        participant_thread.join()
        participant_socket.close()
        coordinator_socket.close()

        conn = self.get_postgres_connection()
        cur = conn.cursor()
        cur.execute(""" SELECT COUNT(*) FROM thermometerobservation; """)
        result = cur.fetchone()
        self.assertEqual(result[0], 1)

    def test_failure_at_prepare(self):
        participant_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        participant_socket.bind((socket.gethostname(), self.test_port + 1))
        participant_socket.listen(5)

        # Connect to our coordinator.
        coordinator_socket = communication.GenericSocketUser()
        coordinator_socket.socket.connect((socket.gethostname(), self.test_port + 1,))
        from_coordinator_socket, from_coordinator_address = participant_socket.accept()
        participant_thread = participate.TransactionParticipantThread(
            **self.get_postgres_context(),
            transaction_id=str(uuid.uuid4()),
            client_socket=from_coordinator_socket,
            protocol_db=self.test_file,
            failure_time=5,
        )
        participant_thread.start()
        time.sleep(0.01)

        coordinator_socket.send_message(OpCode.INSERT_FROM_COORDINATOR, ["""
            INSERT INTO thermometerobservation 
            VALUES ('a239a033-b340-426d-a686-ad32908709ae', 48, '2017-11-08 00:00:00', 
                    '9592a785_d3a4_4de2_bc3d_cfa1a127bf40');
        """])
        self.assertEqual(coordinator_socket.read_message(), [ResponseCode.OK])
        coordinator_socket.send_op(OpCode.PREPARE_TO_COMMIT)
        self.assertEqual(coordinator_socket.read_message(), [ResponseCode.PREPARED_FROM_PARTICIPANT])

        coordinator_socket.send_op(OpCode.ROLLBACK_FROM_COORDINATOR)
        self.assertEqual(coordinator_socket.read_message(), [ResponseCode.ACKNOWLEDGE_END])
        participant_thread.join()
        participant_socket.close()
        coordinator_socket.close()

        conn = self.get_postgres_connection()
        cur = conn.cursor()
        cur.execute(""" SELECT COUNT(*) FROM thermometerobservation; """)
        result = cur.fetchone()
        self.assertEqual(result[0], 0)

    def test_failure_before_prepare(self):
        participant_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        participant_socket.bind((socket.gethostname(), self.test_port + 2))
        participant_socket.listen(5)

        # Connect to our coordinator.
        coordinator_socket = communication.GenericSocketUser()
        coordinator_socket.socket.connect((socket.gethostname(), self.test_port + 2,))
        from_coordinator_socket, from_coordinator_address = participant_socket.accept()
        participant_thread = participate.TransactionParticipantThread(
            **self.get_postgres_context(),
            transaction_id=str(uuid.uuid4()),
            client_socket=from_coordinator_socket,
            protocol_db=self.test_file,
            failure_time=5,
        )
        participant_thread.start()
        time.sleep(0.01)

        coordinator_socket.send_message(OpCode.INSERT_FROM_COORDINATOR, ["""
            INSERT INTO thermometerobservation 
            VALUES ('a239a033-b340-426d-a686-ad32908709ae', 48, '2017-11-08 00:00:00', 
                    '9592a785_d3a4_4de2_bc3d_cfa1a127bf40');
        """])
        self.assertEqual(coordinator_socket.read_message(), [ResponseCode.OK])
        coordinator_socket.send_op(OpCode.ROLLBACK_FROM_COORDINATOR)
        self.assertEqual(coordinator_socket.read_message(), [ResponseCode.ACKNOWLEDGE_END])
        time.sleep(0.01)

        participant_thread.join()
        participant_socket.close()
        coordinator_socket.close()

        conn = self.get_postgres_connection()
        cur = conn.cursor()
        cur.execute(""" SELECT COUNT(*) FROM thermometerobservation; """)
        result = cur.fetchone()
        self.assertEqual(result[0], 0)

    def test_enter_waiting_from_abort(self):
        participant_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        participant_socket.bind((socket.gethostname(), self.test_port + 3))
        participant_socket.listen(5)

        # Connect to our coordinator.
        coordinator_socket_1 = communication.GenericSocketUser()
        coordinator_socket_1.socket.connect((socket.gethostname(), self.test_port + 3,))
        from_coordinator_socket_1, from_coordinator_address = participant_socket.accept()
        participant_thread = participate.TransactionParticipantThread(
            **self.get_postgres_context(),
            transaction_id=str(uuid.uuid4()),
            client_socket=from_coordinator_socket_1,
            protocol_db=self.test_file,
            failure_time=5
        )
        participant_thread.start()
        time.sleep(0.01)

        coordinator_socket_1.send_message(OpCode.INSERT_FROM_COORDINATOR, ["""
            INSERT INTO thermometerobservation 
            VALUES ('a239a033-b340-426d-a686-ad32908709ae', 48, '2017-11-08 00:00:00', 
                    '9592a785_d3a4_4de2_bc3d_cfa1a127bf40');
        """])
        self.assertEqual(coordinator_socket_1.read_message(), [ResponseCode.OK])
        coordinator_socket_1.send_op(OpCode.ROLLBACK_FROM_COORDINATOR)
        coordinator_socket_1.close()
        time.sleep(3)

        # Connect from our coordinator again.
        coordinator_socket_2 = communication.GenericSocketUser()
        coordinator_socket_2.socket.connect((socket.gethostname(), self.test_port + 3,))
        from_coordinator_socket_2, from_coordinator_address = participant_socket.accept()
        participant_thread.inject_socket(from_coordinator_socket_2)
        self.assertEqual(coordinator_socket_2.read_message(), [ResponseCode.ACKNOWLEDGE_END])
        participant_thread.join()
        participant_socket.close()
        coordinator_socket_2.close()
        from_coordinator_socket_2.close()

        conn = self.get_postgres_connection()
        cur = conn.cursor()
        cur.execute(""" SELECT COUNT(*) FROM thermometerobservation; """)
        result = cur.fetchone()
        self.assertEqual(result[0], 0)

    def test_enter_waiting_from_commit(self):
        participant_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        participant_socket.bind((socket.gethostname(), self.test_port + 4))
        participant_socket.listen(5)

        # Connect to our coordinator.
        coordinator_socket_1 = communication.GenericSocketUser()
        coordinator_socket_1.socket.connect((socket.gethostname(), self.test_port + 4,))
        from_coordinator_socket_1, from_coordinator_address = participant_socket.accept()
        participant_thread = participate.TransactionParticipantThread(
            **self.get_postgres_context(),
            transaction_id=str(uuid.uuid4()),
            client_socket=from_coordinator_socket_1,
            protocol_db=self.test_file,
            failure_time=5
        )
        participant_thread.start()
        time.sleep(0.01)

        coordinator_socket_1.send_message(OpCode.INSERT_FROM_COORDINATOR, ["""
            INSERT INTO thermometerobservation
            VALUES ('a239a033-b340-426d-a686-ad32908709ae', 48, '2017-11-08 00:00:00',
                    '9592a785_d3a4_4de2_bc3d_cfa1a127bf40');
        """])
        self.assertEqual(coordinator_socket_1.read_message(), [ResponseCode.OK])
        coordinator_socket_1.send_op(OpCode.PREPARE_TO_COMMIT)
        self.assertEqual(coordinator_socket_1.read_message(), [ResponseCode.PREPARED_FROM_PARTICIPANT])
        coordinator_socket_1.send_op(OpCode.COMMIT_FROM_COORDINATOR)
        coordinator_socket_1.close()
        time.sleep(3)

        # Connect from our coordinator again.
        coordinator_socket_2 = communication.GenericSocketUser()
        coordinator_socket_2.socket.connect((socket.gethostname(), self.test_port + 4,))
        from_coordinator_socket_2, from_coordinator_address = participant_socket.accept()
        participant_thread.inject_socket(from_coordinator_socket_2)
        self.assertEqual(coordinator_socket_2.read_message(), [ResponseCode.ACKNOWLEDGE_END])
        participant_thread.join()
        participant_socket.close()
        coordinator_socket_2.close()

        conn = self.get_postgres_connection()
        cur = conn.cursor()
        cur.execute(""" SELECT COUNT(*) FROM thermometerobservation; """)
        result = cur.fetchone()
        self.assertEqual(result[0], 1)

    def test_enter_waiting_from_prepare(self):
        participant_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        participant_socket.bind((socket.gethostname(), self.test_port + 5))
        participant_socket.listen(5)

        # Connect to our coordinator.
        coordinator_socket_1 = communication.GenericSocketUser()
        coordinator_socket_1.socket.connect((socket.gethostname(), self.test_port + 5,))
        from_coordinator_socket_1, from_coordinator_address = participant_socket.accept()
        participant_thread = participate.TransactionParticipantThread(
            **self.get_postgres_context(),
            transaction_id=str(uuid.uuid4()),
            client_socket=from_coordinator_socket_1,
            protocol_db=self.test_file,
            failure_time=5,
            wait_period=1
        )
        participant_thread.start()
        time.sleep(0.01)

        coordinator_socket_1.send_message(OpCode.INSERT_FROM_COORDINATOR, ["""
            INSERT INTO thermometerobservation
            VALUES ('a239a033-b340-426d-a686-ad32908709ae', 48, '2017-11-08 00:00:00',
                    '9592a785_d3a4_4de2_bc3d_cfa1a127bf40');
        """])
        self.assertEqual(coordinator_socket_1.read_message(), [ResponseCode.OK])
        coordinator_socket_1.send_op(OpCode.PREPARE_TO_COMMIT)
        self.assertEqual(coordinator_socket_1.read_message(), [ResponseCode.PREPARED_FROM_PARTICIPANT])
        coordinator_socket_1.close()
        time.sleep(3)

        # Connect from our coordinator again.
        coordinator_socket_2 = communication.GenericSocketUser()
        coordinator_socket_2.socket.connect((socket.gethostname(), self.test_port + 5,))
        from_coordinator_socket_2, from_coordinator_address = participant_socket.accept()
        participant_thread.inject_socket(from_coordinator_socket_2)
        self.assertEqual(coordinator_socket_2.read_message(), [OpCode.TRANSACTION_STATUS])
        coordinator_socket_2.send_response(ResponseCode.TRANSACTION_COMMITTED)
        time.sleep(0.1)
        self.assertEqual(coordinator_socket_2.read_message(), [ResponseCode.ACKNOWLEDGE_END])
        time.sleep(0.1)
        participant_thread.join()
        participant_socket.close()
        coordinator_socket_2.close()

        conn = self.get_postgres_connection()
        cur = conn.cursor()
        cur.execute(""" SELECT COUNT(*) FROM thermometerobservation; """)
        result = cur.fetchone()
        self.assertEqual(result[0], 1)
