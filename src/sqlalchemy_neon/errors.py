"""
Exception hierarchy for sqlalchemy-neon.

Follows PEP 249 Database API Specification v2.0 exception hierarchy.
"""

from __future__ import annotations


class Error(Exception):
    """Base exception for all database errors.

    This is the base class from which all other exceptions in this module
    derive. It can be used to catch all database-related exceptions.
    """

    pass


class Warning(Exception):
    """Exception raised for important warnings.

    Such as data truncations while inserting, etc.
    """

    pass


class InterfaceError(Error):
    """Exception raised for errors related to the database interface.

    Raised when there is an error in the database module itself,
    not the database.
    """

    pass


class DatabaseError(Error):
    """Exception raised for errors related to the database.

    Base class for errors that are related to the database itself.
    """

    pass


class DataError(DatabaseError):
    """Exception raised for errors due to problems with the processed data.

    Examples include division by zero, numeric value out of range, etc.
    """

    pass


class OperationalError(DatabaseError):
    """Exception raised for errors related to the database's operation.

    Raised for errors that are not necessarily under the control of the
    programmer, e.g. an unexpected disconnect occurs, the data source name
    is not found, a transaction could not be processed, a memory allocation
    error occurred during processing, etc.
    """

    pass


class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database is affected.

    Examples include foreign key check failure, duplicate key, etc.
    """

    pass


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error.

    Examples include cursor not valid anymore, transaction out of sync, etc.
    """

    pass


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors.

    Examples include table not found, syntax error in SQL statement,
    wrong number of parameters specified, etc.
    """

    pass


class NotSupportedError(DatabaseError):
    """Exception raised when a method or database API is not supported.

    Raised when a method or database API was used which is not supported
    by the database, e.g. requesting a .rollback() on a connection that
    does not support transactions.
    """

    pass


# Neon-specific exceptions


class NeonError(Error):
    """Base exception for Neon-specific errors."""

    pass


class NeonConnectionError(NeonError, OperationalError):
    """Exception raised when connection to Neon fails."""

    def __init__(self, message: str, status_code: int | None = None):
        self.status_code = status_code
        super().__init__(message)


class NeonHTTPError(NeonError, OperationalError):
    """Exception raised for HTTP-level errors when communicating with Neon."""

    def __init__(self, message: str, status_code: int, response_body: str | None = None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(f"HTTP {status_code}: {message}")


class NeonAuthenticationError(NeonError, OperationalError):
    """Exception raised when authentication with Neon fails."""

    pass


class NeonQueryError(NeonError, DatabaseError):
    """Exception raised when a query execution fails on Neon."""

    def __init__(
        self,
        message: str,
        code: str | None = None,
        detail: str | None = None,
        hint: str | None = None,
    ):
        self.code = code
        self.detail = detail
        self.hint = hint
        super().__init__(message)


class NeonTransactionError(NeonError, DatabaseError):
    """Exception raised for transaction-related errors."""

    pass


class NeonTypeError(NeonError, DataError):
    """Exception raised for type conversion errors."""

    pass


class NeonConfigurationError(NeonError, InterfaceError):
    """Exception raised for configuration errors."""

    pass
