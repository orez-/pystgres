class PostgresError(Exception):
    """
    Superclass for Postgres errors.
    """


class UndefinedTableError(PostgresError):
    """Relation does not exist."""
    error_code = '42P01'

    def __init__(self, message=None, *, table=None):
        super().__init__()
        self._message = message
        self._table = table

    def __str__(self):
        if self._message:
            return self._message
        return f'relation "{self._table}" does not exist'


class NotNullViolation(PostgresError):
    error_code = '23502'


class DuplicateAliasError(PostgresError):
    error_code = '42712'

    def __init__(self, table):
        super().__init__()
        self._table = table

    def __str__(self):
        return f'table name "{self._table!r}" specified more than once'


class UndefinedColumnError(PostgresError):
    error_code = '42703'


class AmbiguousColumnError(PostgresError):
    error_code = '42702'


class AmbiguousTableError(PostgresError):
    error_code = '42P09'


class InvalidEscapeSequence(PostgresError):
    error_code = '22025'


class UndefinedFunctionError(PostgresError):
    error_code = '42883'


class InvalidSchemaNameError(PostgresError):
    error_code = '3F000'


class PostgresSyntaxError(PostgresError):
    error_code = '42601'
