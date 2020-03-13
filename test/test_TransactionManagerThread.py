import psycopg2.extensions
import communication
import unittest
import psycopg2
import protocol
import manager
import logging
import socket
import time
import uuid
import json
import os

from typing import Union
from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class _TestDummyTransactionRoleFactory(manager.TransactionRoleAbstractFactory):
    def get_coordinator(self, client_socket: Union[socket.socket, None]):
        logger.info("Spawning coordinator.")
        if client_socket is not None:
            client_socket.close()

        class _DummyCoordinator(object):
            transaction_id = uuid.uuid4()

            @staticmethod
            def start():
                pass

        return _DummyCoordinator()

    def get_participant(self, coordinator_id: int, transaction_id: bytes, client_socket: socket.socket):
        logger.info("Spawning participant.")
        if client_socket is not None:
            client_socket.close()

        class _DummyParticipant(object):
            @staticmethod
            def start():
                pass

        return _DummyParticipant()


class _TestCoordinatorRecoveryTransactionStateFactory(manager.TransactionRoleAbstractFactory):
    def __init__(self, **context):
        super().__init__()
        self.context = context

    def get_coordinator(self, client_socket: Union[socket.socket, None]):
        logger.info("Spawning coordinator.")
        if client_socket is not None:
            client_socket.close()

        class _DummyCoordinator(object):
            active_map = {}
            transaction_id = None
            state = None

            def start(self):
                for v in self.active_map.values():
                    dummy_socket = communication.GenericSocketUser()
                    dummy_socket.send_op(OpCode.SHUTDOWN, v)
                    time.sleep(0.1)
                    dummy_socket.close()
                    v.close()

        return _DummyCoordinator()

    def get_participant(self, coordinator_id: int, transaction_id: bytes, client_socket: socket.socket):
        pass


class _TestParticipantRecoveryTransactionStateFactory(manager.TransactionRoleAbstractFactory):
    def __init__(self, **context):
        super().__init__()
        self.context = context

    def get_coordinator(self, client_socket: Union[socket.socket, None]):
        pass

    def get_participant(self, coordinator_id: int, transaction_id: bytes, client_socket: socket.socket):
        logger.info("Spawning participant.")
        context = self.context

        class _DummyParticipant(object):
            state = None

            def __init__(self):
                self.site_list = context['site_list']

            @staticmethod
            def start():
                dummy_socket = communication.GenericSocketUser()
                dummy_socket.send_op(OpCode.SHUTDOWN, client_socket)
                time.sleep(0.1)
                dummy_socket.close()
                client_socket.close()

        return _DummyParticipant()


class TestTransactionManagerThread(unittest.TestCase):
    test_file = 'test_database.log'
    test_port = 52000

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

    def tearDown(self) -> None:
        try:
            os.remove(self.test_file)
            os.remove(self.test_file + '1')
        except OSError:
            pass

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            conn = cls.get_postgres_connection()
            cur = conn.cursor()
            cur.execute("""
                TRUNCATE TABLE thermometerobservation ;
            """)
            cur.commit()

        except psycopg2.Error as e:
            pass

    def test_open_close(self):
        # Spawn and start our manager threads.
        manager_thread_1 = manager.TransactionManagerThread(
            **self.get_postgres_context(),
            protocol_db=self.test_file,
            role_factory=_TestDummyTransactionRoleFactory(),
            site_alias=socket.gethostname(),
            node_port=self.test_port,
            site_list=[
                {'hostname': socket.gethostname(), 'port': self.test_port},
                {'hostname': socket.gethostname(), 'port': self.test_port + 1}
            ]
        )
        manager_thread_2 = manager.TransactionManagerThread(
            **self.get_postgres_context(),
            protocol_db=self.test_file,
            role_factory=_TestDummyTransactionRoleFactory(),
            site_alias=socket.gethostname(),
            node_port=self.test_port + 1,
            site_list=[
                {'hostname': socket.gethostname(), 'port': self.test_port},
                {'hostname': socket.gethostname(), 'port': self.test_port + 1}
            ]
        )
        manager_thread_1.start()
        manager_thread_2.start()
        time.sleep(0.5)

        # Connect to TM_1.
        client_socket = communication.GenericSocketUser()
        client_socket.socket.connect((socket.gethostname(), self.test_port))
        client_socket.send_op(OpCode.SHUTDOWN)
        time.sleep(0.5)  # Wait for TM_1 to acknowledge.
        client_socket.socket.close()

        # Connect to TM_2.
        client_socket = communication.GenericSocketUser()
        client_socket.socket.connect((socket.gethostname(), self.test_port + 1))
        client_socket.send_op(OpCode.SHUTDOWN)
        time.sleep(0.5)  # Wait for TM_2 to acknowledge.
        client_socket.socket.close()

        manager_thread_1.join()
        manager_thread_2.join()

    def test_start_transaction(self):
        manager_thread = manager.TransactionManagerThread(
            **self.get_postgres_context(),
            protocol_db=self.test_file,
            role_factory=_TestDummyTransactionRoleFactory(),
            site_alias=socket.gethostname(),
            node_port=self.test_port + 2,
            site_list=[
                {'alias': socket.gethostname(), 'hostname': socket.gethostname(), 'port': self.test_port + 2}
            ]
        )
        manager_thread.start()
        time.sleep(0.5)

        # Connect to TM.
        client_socket = communication.GenericSocketUser()
        client_socket.socket.connect((socket.gethostname(), self.test_port + 2))
        client_socket.send_op(OpCode.START_TRANSACTION)
        time.sleep(0.01)
        client_socket.socket.close()

        # Create new connection to TM, and issue the shutdown.
        client_socket = communication.GenericSocketUser()
        client_socket.socket.connect((socket.gethostname(), self.test_port + 2))
        client_socket.send_op(OpCode.SHUTDOWN)
        time.sleep(0.5)
        client_socket.socket.close()
        manager_thread.join()

    def test_participate_in_transaction(self):
        manager_thread = manager.TransactionManagerThread(
            **self.get_postgres_context(),
            protocol_db=self.test_file,
            role_factory=_TestDummyTransactionRoleFactory(),
            site_alias=socket.gethostname(),
            node_port=self.test_port + 3,
            site_list=[
                {'hostname': socket.gethostname(), 'port': self.test_port + 3}
            ]
        )
        manager_thread.start()
        time.sleep(0.5)

        transaction_id = str(uuid.uuid4())
        other_manager_socket = communication.GenericSocketUser()
        other_manager_socket.socket.connect((socket.gethostname(), self.test_port + 3))
        other_manager_socket.send_message(OpCode.INITIATE_PARTICIPANT, [transaction_id, 0])
        time.sleep(0.01)
        other_manager_socket.socket.close()

        # Create new connection to TM, and issue the shutdown.
        client_socket = communication.GenericSocketUser()
        client_socket.socket.connect((socket.gethostname(), self.test_port + 3))
        client_socket.send_op(OpCode.SHUTDOWN)
        time.sleep(0.5)
        client_socket.socket.close()
        manager_thread.join()

    def test_commit_from_coordinator_no_knowledge(self):
        manager_thread = manager.TransactionManagerThread(
            **self.get_postgres_context(),
            protocol_db=self.test_file,
            role_factory=_TestDummyTransactionRoleFactory(),
            site_alias=socket.gethostname(),
            node_port=self.test_port + 4,
            site_list=[
                {'hostname': socket.gethostname(), 'port': self.test_port + 4}
            ]
        )
        manager_thread.start()
        time.sleep(0.5)

        transaction_id = str(uuid.uuid4())
        other_manager_socket = communication.GenericSocketUser()
        other_manager_socket.socket.connect((socket.gethostname(), self.test_port + 4))
        other_manager_socket.send_message(OpCode.COMMIT_FROM_COORDINATOR, [transaction_id])

        response = other_manager_socket.read_message()
        self.assertEqual(response, [ResponseCode.ACKNOWLEDGE_END])
        other_manager_socket.socket.close()

        # Create new connection to TM, and issue the shutdown.
        client_socket = communication.GenericSocketUser()
        client_socket.socket.connect((socket.gethostname(), self.test_port + 4))
        client_socket.send_op(OpCode.SHUTDOWN)
        time.sleep(0.5)
        client_socket.socket.close()
        manager_thread.join()

    def test_recovery_coordinator(self):
        transaction_id_1 = str(uuid.uuid4())
        conn = self.get_postgres_connection()
        conn.tpc_begin(psycopg2.extensions.Xid.from_string(transaction_id_1))
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO thermometerobservation 
            VALUES ('a239a033-b340-426d-a686-ad32908709ae', 48, '2017-11-08 00:00:00', 
                    '9592a785_d3a4_4de2_bc3d_cfa1a127bf40');
        """)

        # Coordinator knows COMMIT, but does not have the ACK.
        pdb1 = protocol.ProtocolDatabase(self.test_file + '1')
        pdb1.log_initialize_of(transaction_id_1, TransactionRole.COORDINATOR)
        pdb1.add_participant(transaction_id_1, 1)
        pdb1.log_commit_of(transaction_id_1)
        conn.tpc_prepare()
        pdb1.close()

        class TestRM(object):
            def tpc_recover(self):
                return []

            def close(self):
                pass

        manager_thread_1 = manager.TransactionManagerThread(
            **self.get_postgres_context(),
            protocol_db=self.test_file + '1',
            role_factory=_TestCoordinatorRecoveryTransactionStateFactory(transaction_id_1=transaction_id_1),
            site_alias=socket.gethostname(),
            node_port=self.test_port + 5,
            site_list=[
                {'alias': socket.gethostname(), 'hostname': socket.gethostname(), 'port': self.test_port + 5},
                {'alias': socket.gethostname(), 'hostname': socket.gethostname(), 'port': self.test_port + 6}
            ]
        )
        manager_thread_2 = manager.TransactionManagerThread(
            **self.get_postgres_context(),
            test_rm=TestRM(),
            protocol_db=self.test_file,
            role_factory=_TestDummyTransactionRoleFactory(),
            site_alias=socket.gethostname(),
            node_port=self.test_port + 6,
            site_list=[
                {'alias': socket.gethostname(), 'hostname': socket.gethostname(), 'port': self.test_port + 5},
                {'alias': socket.gethostname(), 'hostname': socket.gethostname(), 'port': self.test_port + 6}
            ]
        )

        manager_thread_2.start()
        time.sleep(1.0)
        manager_thread_1.start()
        time.sleep(0.5)

        # Create new connection to TM, and issue the shutdown.
        client_socket = communication.GenericSocketUser()
        client_socket.socket.connect((socket.gethostname(), self.test_port + 5))
        client_socket.send_op(OpCode.SHUTDOWN)
        time.sleep(0.5)
        client_socket.socket.close()

        manager_thread_1.join()
        manager_thread_2.join()
