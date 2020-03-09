""" This file contains all shared code / constants for this codebase. """
from enum import IntEnum


class OpCode(IntEnum):
    """ Operation codes. """
    STOP = -1
    NO_OP = 0

    # Between the coordinator and the client.
    START_TRANSACTION = 1
    ABORT_TRANSACTION = 2
    COMMIT_TRANSACTION = 3
    INSERT_FROM_CLIENT = 4
    SHUTDOWN = 5

    # Between the coordinator and a participant.
    INITIATE_PARTICIPANT = 6
    INSERT_FROM_COORDINATOR = 7
    PREPARE_TO_COMMIT = 8
    COMMIT_FROM_COORDINATOR = 9
    ROLLBACK_FROM_COORDINATOR = 10

    # For recovery use when a TM fails.
    TRANSACTION_STATUS = 11


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
