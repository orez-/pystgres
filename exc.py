class NoSuchRelationError(Exception):
    """Relation does not exist."""


class IntegrityConstraintViolation(Exception):
    """Integrity constraint was violated."""


class DuplicateAliasError(Exception):
    error_code = 42712


class UndefinedColumnError(Exception):
    error_code = 42703


class AmbiguousColumnError(Exception):
    error_code = 42702
