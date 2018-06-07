class PostgresError(Exception):
    """
    Superclass for Postgres errors.
    """


class UndefinedTableError(PostgresError):
    """Relation does not exist."""
    error_code = '42P01'

    def __init__(self, table):
        super().__init__()
        self._table = table

    def __str__(self):
        return f'relation "{self._table}" does not exist'


class NotNullViolation(PostgresError):
    error_code = '23502'


class DuplicateAliasError(PostgresError):
    error_code = '42712'


class UndefinedColumnError(PostgresError):
    error_code = '42703'


class AmbiguousColumnError(PostgresError):
    error_code = '42702'
