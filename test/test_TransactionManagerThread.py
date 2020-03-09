import socket
import logging
import unittest
import time
import manager
import communication
import uuid

from typing import Union
from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class _TestNoOpTransactionStateFactory(manager.TransactionStateAbstractFactory):
    def get_coordinator(self, client_socket: Union[socket.socket, None]):
        logger.info("Spawning coordinator.")
        if client_socket is not None:
            client_socket.close()

        class _DummyCoordinator(object):
            transaction_id = uuid.uuid4().bytes

            @staticmethod
            def start():
                pass

        return _DummyCoordinator()

    def get_participant(self, transaction_id: bytes, client_socket: socket.socket):
        logger.info("Spawning participant.")
        if client_socket is not None:
            client_socket.close()

        class _DummyParticipant(object):
            @staticmethod
            def start():
                pass

        return _DummyParticipant()

    def get_wal(self):
        class _NoUncommittedTransactionsWAL(object):
            @staticmethod
            def get_uncommitted_transactions():
                return []

        return _NoUncommittedTransactionsWAL()


class _TestCoordinatorRecoveryTransactionStateFactory(manager.TransactionStateAbstractFactory):
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

    def get_participant(self, transaction_id: bytes, client_socket: socket.socket):
        pass

    def get_wal(self):
        transaction_id_1 = self.context['transaction_id_1']

        class _WALWithUncommittedTransaction(object):
            @staticmethod
            def get_role_in(transaction_id: bytes):
                return TransactionRole.COORDINATOR

            @staticmethod
            def get_participants_in(transaction_id: bytes):
                return [1]

            @staticmethod
            def get_uncommitted_transactions():
                return [(transaction_id_1, "P",)]

        return _WALWithUncommittedTransaction()


class _TestParticipantRecoveryTransactionStateFactory(manager.TransactionStateAbstractFactory):
    def __init__(self, **context):
        super().__init__()
        self.context = context

    def get_coordinator(self, client_socket: Union[socket.socket, None]):
        pass

    def get_participant(self, transaction_id: bytes, client_socket: socket.socket):
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

    def get_wal(self):
        transaction_id_1 = self.context['transaction_id_1']

        class _WALWithUncommittedTransaction(object):
            @staticmethod
            def get_role_in(transaction_id: bytes):
                return TransactionRole.PARTICIPANT

            @staticmethod
            def get_coordinator_for(transaction_id: bytes):
                return 1

            @staticmethod
            def get_uncommitted_transactions():
                return [(transaction_id_1, "P",)]

        return _WALWithUncommittedTransaction()


class TestTransactionManagerThread(unittest.TestCase):
    """ Verifies the class that acts as our transaction manager (i.e. the TM daemon). """
    test_port = 52000

    def test_open_close(self):
        # Spawn and start our manager threads.
        manager_thread_1 = manager._TransactionManagerThread(
            state_factory=_TestNoOpTransactionStateFactory(),
            hostname=socket.gethostname(),
            node_port=self.test_port,
            site_list=[
                {'hostname': socket.gethostname(), 'port': self.test_port},
                {'hostname': socket.gethostname(), 'port': self.test_port + 1}
            ]
        )
        manager_thread_2 = manager._TransactionManagerThread(
            state_factory=_TestNoOpTransactionStateFactory(),
            hostname=socket.gethostname(),
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
        manager_thread = manager._TransactionManagerThread(
            state_factory=_TestNoOpTransactionStateFactory(),
            hostname=socket.gethostname(),
            node_port=self.test_port + 2,
            site_list=[
                {'hostname': socket.gethostname(), 'port': self.test_port + 2}
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
        manager_thread = manager._TransactionManagerThread(
            state_factory=_TestNoOpTransactionStateFactory(),
            hostname=socket.gethostname(),
            node_port=self.test_port + 3,
            site_list=[
                {'hostname': socket.gethostname(), 'port': self.test_port + 3}
            ]
        )
        manager_thread.start()
        time.sleep(0.5)

        transaction_id = uuid.uuid4().bytes
        other_manager_socket = communication.GenericSocketUser()
        other_manager_socket.socket.connect((socket.gethostname(), self.test_port + 3))
        other_manager_socket.send_message(OpCode.INITIATE_PARTICIPANT, [transaction_id])
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
        manager_thread = manager._TransactionManagerThread(
            state_factory=_TestNoOpTransactionStateFactory(),
            hostname=socket.gethostname(),
            node_port=self.test_port + 4,
            site_list=[
                {'hostname': socket.gethostname(), 'port': self.test_port + 4}
            ]
        )
        manager_thread.start()
        time.sleep(0.5)

        transaction_id = uuid.uuid4().bytes
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

    def test_coordinator_recovery(self):
        transaction_id_1 = uuid.uuid4().bytes
        manager_thread_1 = manager._TransactionManagerThread(
            state_factory=_TestCoordinatorRecoveryTransactionStateFactory(transaction_id_1=transaction_id_1),
            hostname=socket.gethostname(),
            node_port=self.test_port + 5,
            site_list=[
                {'hostname': socket.gethostname(), 'port': self.test_port + 5},
                {'hostname': socket.gethostname(), 'port': self.test_port + 6}
            ]
        )
        manager_thread_2 = manager._TransactionManagerThread(
            state_factory=_TestNoOpTransactionStateFactory(),
            hostname=socket.gethostname(),
            node_port=self.test_port + 6,
            site_list=[
                {'hostname': socket.gethostname(), 'port': self.test_port + 5},
                {'hostname': socket.gethostname(), 'port': self.test_port + 6}
            ]
        )

        manager_thread_2.start()
        time.sleep(0.5)
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

    def test_participant_recovery(self):
        transaction_id_1 = uuid.uuid4().bytes
        manager_thread_1 = manager._TransactionManagerThread(
            state_factory=_TestParticipantRecoveryTransactionStateFactory(
                site_list=[
                    {'hostname': socket.gethostname(), 'port': self.test_port + 7},
                    {'hostname': socket.gethostname(), 'port': self.test_port + 8}
                ],
                transaction_id_1=transaction_id_1
            ),
            hostname=socket.gethostname(),
            node_port=self.test_port + 7,
            site_list=[
                {'hostname': socket.gethostname(), 'port': self.test_port + 7},
                {'hostname': socket.gethostname(), 'port': self.test_port + 8}
            ]
        )
        manager_thread_2 = manager._TransactionManagerThread(
            state_factory=_TestNoOpTransactionStateFactory(),
            hostname=socket.gethostname(),
            node_port=self.test_port + 8,
            site_list=[
                {'hostname': socket.gethostname(), 'port': self.test_port + 7},
                {'hostname': socket.gethostname(), 'port': self.test_port + 8}
            ]
        )

        manager_thread_2.start()
        time.sleep(0.5)
        manager_thread_1.start()
        time.sleep(0.5)

        # Create new connection to TM, and issue the shutdown.
        client_socket = communication.GenericSocketUser()
        client_socket.socket.connect((socket.gethostname(), self.test_port + 7))
        client_socket.send_op(OpCode.SHUTDOWN)
        time.sleep(0.5)
        client_socket.socket.close()

        manager_thread_1.join()
        manager_thread_2.join()
