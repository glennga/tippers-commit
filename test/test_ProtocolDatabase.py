import unittest
import protocol
import logging
import uuid
import time
import os
import re

from shared import *

# We maintain a module-level logger.
logger = logging.getLogger(__name__)


class TestProtocolDatabase(unittest.TestCase):
    test_file = 'test_database.log'

    def tearDown(self) -> None:
        try:
            os.remove('coordinator_' + self.test_file)
        except OSError:
            pass

        try:
            os.remove('participant_' + self.test_file)
        except OSError:
            pass

    @staticmethod
    def _strip_whitespace(text):
        return re.sub(r'\s+', '', text.strip().replace('\n', ''))

    def test_transaction_commit(self):
        transaction_id = str(uuid.uuid4())

        coordinator_pdb = protocol.ProtocolDatabase('coordinator_' + self.test_file)
        participant_pdb = protocol.ProtocolDatabase('participant_' + self.test_file)
        coordinator_pdb.log_initialize_of(transaction_id, TransactionRole.COORDINATOR)
        participant_pdb.log_initialize_of(transaction_id, TransactionRole.PARTICIPANT)

        self.assertEqual(coordinator_pdb.get_role_in(transaction_id), TransactionRole.COORDINATOR)
        self.assertEqual(participant_pdb.get_role_in(transaction_id), TransactionRole.PARTICIPANT)

        coordinator_pdb.log_commit_of(transaction_id)
        participant_pdb.log_commit_of(transaction_id)

        coordinator_pdb.close()
        participant_pdb.close()
        time.sleep(0.5)

    def test_transaction_abort(self):
        transaction_id = str(uuid.uuid4())

        coordinator_pdb = protocol.ProtocolDatabase('coordinator_' + self.test_file)
        participant_pdb = protocol.ProtocolDatabase('participant_' + self.test_file)
        coordinator_pdb.log_initialize_of(transaction_id, TransactionRole.COORDINATOR)
        participant_pdb.log_initialize_of(transaction_id, TransactionRole.PARTICIPANT)

        self.assertEqual(coordinator_pdb.get_role_in(transaction_id), TransactionRole.COORDINATOR)
        self.assertEqual(participant_pdb.get_role_in(transaction_id), TransactionRole.PARTICIPANT)

        coordinator_pdb.log_abort_of(transaction_id)
        participant_pdb.log_abort_of(transaction_id)

        coordinator_pdb.close()
        participant_pdb.close()
        time.sleep(0.5)

    def test_site_awareness(self):
        transaction_id = str(uuid.uuid4())

        coordinator_pdb = protocol.ProtocolDatabase('coordinator_' + self.test_file)
        participant_pdb = protocol.ProtocolDatabase('participant_' + self.test_file)
        coordinator_pdb.log_initialize_of(transaction_id, TransactionRole.COORDINATOR)
        participant_pdb.log_initialize_of(transaction_id, TransactionRole.PARTICIPANT)

        coordinator_pdb.add_participant(transaction_id, 1)
        coordinator_pdb.add_participant(transaction_id, 2)
        participant_pdb.add_coordinator(transaction_id, 2)

        participants = coordinator_pdb.get_participants_in(transaction_id)
        self.assertEqual(len(participants), 2)
        self.assertIn(1, participants)
        self.assertIn(2, participants)

        coordinator = participant_pdb.get_coordinator_for(transaction_id)
        self.assertEqual(coordinator, 2)

        coordinator_pdb.close()
        participant_pdb.close()
        time.sleep(0.5)

    def test_transaction_recovery(self):
        transaction_id_1 = str(uuid.uuid4())
        transaction_id_2 = str(uuid.uuid4())

        coordinator_pdb = protocol.ProtocolDatabase('coordinator_' + self.test_file)
        coordinator_pdb.log_initialize_of(transaction_id_1, TransactionRole.COORDINATOR)
        coordinator_pdb.log_initialize_of(transaction_id_2, TransactionRole.COORDINATOR)
        coordinator_pdb.log_prepare_of(transaction_id_1)

        abortable_transactions = coordinator_pdb.get_abortable_transactions()
        self.assertEqual(len(abortable_transactions), 1)
        self.assertEqual(abortable_transactions[0], transaction_id_2)

        coordinator_pdb.close()
        time.sleep(0.5)


if __name__ == "__main__":
    import sys

    logging.basicConfig(stream=sys.stderr)
    unittest.main()
