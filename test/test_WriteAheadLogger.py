import unittest
import os

from shared import *


class TestWriteAheadLogger(unittest.TestCase):
    """ Verifies the class that allows recovery to be possible. """
    logger = logging.getLogger(__qualname__)
    test_file = 'test_wal.log'

    def tearDown(self) -> None:
        try:
            os.remove('coordinator_' + self.test_file)
        except OSError:
            pass

        try:
            os.remove('participant_' + self.test_file)
        except OSError:
            pass

    def test_transaction_metadata(self):
        transaction_id = uuid.uuid4().bytes

        wal_coordinator = WriteAheadLogger('coordinator_' + self.test_file)
        wal_participant = WriteAheadLogger('participant_' + self.test_file)
        wal_coordinator.initialize_transaction(transaction_id, TransactionRole.COORDINATOR)
        wal_participant.initialize_transaction(transaction_id, TransactionRole.PARTICIPANT)

        self.assertEqual(wal_coordinator.get_role_in(transaction_id), TransactionRole.COORDINATOR)
        self.assertEqual(wal_participant.get_role_in(transaction_id), TransactionRole.PARTICIPANT)
        self.assertFalse(wal_coordinator.is_transaction_prepared(transaction_id))
        self.assertFalse(wal_participant.is_transaction_prepared(transaction_id))

        uncommitted_coordinator_transactions = wal_coordinator.get_uncommitted_transactions()
        uncommitted_participant_transactions = wal_participant.get_uncommitted_transactions()
        self.assertEqual(len(uncommitted_coordinator_transactions), 1)
        self.assertEqual(len(uncommitted_participant_transactions), 1)
        self.assertEqual(uncommitted_coordinator_transactions[0], transaction_id)
        self.assertEqual(uncommitted_participant_transactions[0], transaction_id)

        wal_coordinator.log_commit_of(transaction_id)
        wal_participant.log_commit_of(transaction_id)
        uncommitted_coordinator_transactions = wal_coordinator.get_uncommitted_transactions()
        uncommitted_participant_transactions = wal_participant.get_uncommitted_transactions()
        self.assertEqual(len(uncommitted_coordinator_transactions), 0)
        self.assertEqual(len(uncommitted_participant_transactions), 0)

        wal_coordinator.flush_log()
        wal_participant.flush_log()

    def test_participant(self):
        transaction_id = uuid.uuid4().bytes
        wal_coordinator = WriteAheadLogger('coordinator_' + self.test_file)
        wal_coordinator.initialize_transaction(transaction_id, TransactionRole.COORDINATOR)

        wal_coordinator.add_participant(transaction_id, 1)
        wal_coordinator.add_participant(transaction_id, 2)

        participants = wal_coordinator.get_participants_in(transaction_id)
        self.assertEqual(len(participants), 2)
        self.assertIn(1, participants)
        self.assertIn(2, participants)

        wal_coordinator.flush_log()

    def test_redo(self):
        transaction_id = uuid.uuid4().bytes

        wal_coordinator = WriteAheadLogger('coordinator_' + self.test_file)
        wal_participant = WriteAheadLogger('participant_' + self.test_file)
        wal_coordinator.initialize_transaction(transaction_id, TransactionRole.COORDINATOR)
        wal_participant.initialize_transaction(transaction_id, TransactionRole.PARTICIPANT)

        statement_set = [
            """
            INSERT INTO thermometerobservation 
            VALUES ('a239a033-b340-426d-a686-ad32908709ae', 48, '2017-11-08 00:00:00', 
                   '9592a785_d3a4_4de2_bc3d_cfa1a127bf40');
            """,
            """
            INSERT INTO thermometerobservation 
            VALUES ('0af2022c-ab97-4ee6-b502-33052409a6a9', 88, '2017-11-08 00:00:00', 
                    'f2c66e44_fd4a_42bf_8d9d_01d8f4c7b6c1');
            """,
            """
            INSERT INTO thermometerobservation 
            VALUES ('40145f92-0465-4a5f-9513-855128498600', 38, '2017-11-08 00:00:00', 
                    '816cfc04_a67c_4b15_9d6f_313f3c53b761');
            """
        ]

        for wal in [wal_coordinator, wal_participant]:
            wal.log_statement(transaction_id, statement_set[0])
            wal.log_statement(transaction_id, statement_set[1])
            wal.log_statement(transaction_id, statement_set[2])

        coordinator_undo = wal_coordinator.get_undo_for(transaction_id)
        participant_undo = wal_participant.get_undo_for(transaction_id)

        self.assertEqual(len(coordinator_undo), 3)
        self.assertEqual(len(participant_undo), 3)
        for undo in [coordinator_undo, participant_undo]:
            for i, _id in enumerate(statement_set):
                self.assertEqual(undo[i].strip(), statement_set[i].strip())

        wal_coordinator.flush_log()
        wal_participant.flush_log()

    def test_undo(self):
        transaction_id = uuid.uuid4().bytes

        wal_coordinator = WriteAheadLogger('coordinator_' + self.test_file)
        wal_participant = WriteAheadLogger('participant_' + self.test_file)
        wal_coordinator.initialize_transaction(transaction_id, WriteAheadLogger.Role.COORDINATOR)
        wal_participant.initialize_transaction(transaction_id, WriteAheadLogger.Role.PARTICIPANT)

        for wal in [wal_coordinator, wal_participant]:
            wal.log_statement(transaction_id, """
                INSERT INTO thermometerobservation 
                VALUES ('a239a033-b340-426d-a686-ad32908709ae', 48, '2017-11-08 00:00:00', 
                       '9592a785_d3a4_4de2_bc3d_cfa1a127bf40');
            """)
            wal.log_statement(transaction_id, """
                INSERT INTO thermometerobservation 
                VALUES ('0af2022c-ab97-4ee6-b502-33052409a6a9', 88, '2017-11-08 00:00:00', 
                        'f2c66e44_fd4a_42bf_8d9d_01d8f4c7b6c1');
            """)
            wal.log_statement(transaction_id, """
                INSERT INTO thermometerobservation 
                VALUES ('40145f92-0465-4a5f-9513-855128498600', 38, '2017-11-08 00:00:00', 
                        '816cfc04_a67c_4b15_9d6f_313f3c53b761');
            """)

        coordinator_undo = wal_coordinator.get_undo_for(transaction_id)
        participant_undo = wal_participant.get_undo_for(transaction_id)

        self.assertEqual(len(coordinator_undo), 3)
        self.assertEqual(len(participant_undo), 3)
        for undo in [coordinator_undo, participant_undo]:
            for i, _id in enumerate(['40145f92-0465-4a5f-9513-855128498600',
                                     '0af2022c-ab97-4ee6-b502-33052409a6a9',
                                     'a239a033-b340-426d-a686-ad32908709ae']):
                self.assertEqual(undo[i].strip(), f"""
                    DELETE FROM thermometerobservation
                    WHERE id = '{_id}';
                """.strip())

        wal_coordinator.flush_log()
        wal_participant.flush_log()


if __name__ == "__main__":
    import sys

    logging.basicConfig(stream=sys.stderr)
    unittest.main()
