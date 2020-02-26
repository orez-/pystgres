from __future__ import generator_stop

import collections
import functools
import numbers
import operator
import re
import traceback
import typing

import attr
import frozendict as frozendict_lib
import psqlparse

import exc


class frozendict(frozendict_lib.frozendict):
    """
    Specialized frozendict subclass to dedupe copied frozendicts.
    """
    def __new__(cls, *args, **kwargs):
        if args and not kwargs and type(args[0]) == cls:
            return args[0]
        return super(frozendict, cls).__new__(cls)


def first(iterable):
    return next(iter(iterable))


def one(iterable):
    only, = iterable
    return only


def apply(agg_fn):
    """
    Decorator to apply a given function all outputs of the decorated function.

    Useful for functions with many returns that require an unconditional
    transformation, or for generator functions that should always return
    a list or other aggregation.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            return agg_fn(fn(*args, **kwargs))
        return inner
    return decorator


def is_descriptor(obj):
    """Return True if obj is a descriptor, False otherwise."""
    return (
        hasattr(obj, '__get__') or
        hasattr(obj, '__set__') or
        hasattr(obj, '__delete__')
    )


def public_fields(obj):
    """
    Fetch the public, non-method fields and their values from an object.
    """
    return (
        (field, getattr(obj, field))
        for field in dir(obj)
        if not field.startswith('_')
        and not is_descriptor(getattr(obj, field))
    )


def verify_implemented(psql_node, implemented_fields=(), *, expected_values=()):
    """
    Raise NotImplementedError if psql_node has any unexpected non-None public fields.
    """
    expected_fields = {'location'}
    implemented_fields = set(implemented_fields) | expected_fields
    not_implemented_fields = [
        (field, value)
        for field, value in public_fields(psql_node)
        if field not in implemented_fields
        and (field not in expected_values or expected_values[field] != value)
        and value is not None
    ]
    if not_implemented_fields:
        raise NotImplementedError(
            "{}({})".format(
                type(psql_node).__name__,
                ', '.join(f"{field}={value!r}" for field, value in not_implemented_fields)
            )
        )


class AbstractRow(frozendict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._address_extra()

    def _address_missing(self):
        """
        Fill defaults of missing columns, or raise if a missing column has no default.
        """
        for column in type(self).columns:
            if column not in self:
                raise exc.NotNullViolation(
                    f"null value in column {column!r} violates not-null constraint\n"
                    f"Failing row contains ({', '.join(self.values())})."
                )

    def _address_extra(self):
        extra = set(self) - set(type(self).columns)
        if extra:
            raise exc.UndefinedColumnError(
                f"column {first(extra)} of relation \"?\" does not exist"
            )

    @classmethod
    def null_row(cls):
        return cls({col: None for col in cls.columns})

    def __getattr__(self, key):
        return self[key]


@attr.s(slots=True, frozen=True)
class Table:
    schema = attr.ib()
    relname = attr.ib()
    rowtype = attr.ib(repr=False)
    rows = attr.ib(default=(), repr=False)

    def insert(self, rows):
        # TODO: constraints
        return Table(
            schema=self.schema,
            relname=self.relname,
            rowtype=self.rowtype,
            rows=self.rows + tuple(rows),
        )

    @classmethod
    def generate_rowtype(cls, column_data):
        columns = [
            column.colname
            for column in column_data
        ]
        return type('Row', (AbstractRow,), {'columns': columns})


@attr.s(slots=True, frozen=True)
class ResultSet:
    row_names = attr.ib()
    rows = attr.ib(converter=list)


class Element(typing.NamedTuple):
    value: typing.Any
    name: str = None

    def eval(self, row):
        return self.value(row)


class Constant(typing.NamedTuple):
    value: typing.Any
    name: str = None

    def eval(self, row):
        del row
        return self.value


@attr.s(frozen=True, slots=True)
class Schema:
    tables = attr.ib(default=(), converter=frozendict)
    functions = attr.ib(default=(), converter=frozendict)
    types = attr.ib(default=(), converter=frozendict)


@attr.s(frozen=True, slots=True)
class Function:
    fn = attr.ib()


@attr.s(frozen=True, slots=True)
class SortByStrategy:
    _sortby_dir = attr.ib(repr=False)
    _sortby_nulls = attr.ib(repr=False)
    asc = attr.ib(init=False)
    desc = attr.ib(init=False)
    nulls_last = attr.ib(init=False)
    nulls_first = attr.ib(init=False)

    @asc.default
    def _(self):
        return self._sortby_dir != 2

    @desc.default
    def _(self):
        return not self.asc

    @nulls_last.default
    def _(self):
        if self._sortby_nulls == 0:
            return self.asc
        return self._sortby_nulls == 2

    @nulls_first.default
    def _(self):
        return not self.nulls_last


@attr.s(eq=False, order=False)
class SortByKey:
    strat = attr.ib()
    value = attr.ib()

    def _cmp_compatible(self, other):
        """
        Return if `self` is comparable to `other`.
        """
        return (
            type(other) == type(self)
            and self.strat == other.strat
        )

    def __lt__(self, other):
        if not self._cmp_compatible(other):
            return NotImplemented
        if self.value == other.value:
            return False
        if self.value is None:
            return self.strat.nulls_first
        if other.value is None:
            return self.strat.nulls_last
        return (self.value < other.value) == self.strat.asc

    def __eq__(self, other):
        if not self._cmp_compatible(other):
            return NotImplemented
        return self.value == other.value


@attr.s(frozen=True, slots=True)
class PgType:
    converter = attr.ib()
    description = attr.ib()


def create_pg_catalog():
    integer = PgType(
        converter=int,
        description="-2 billion to 2 billion integer, 4-byte storage",  # TODO: ha ha ha
    )

    def pg_bool(value):
        if value is None:
            return None
        if isinstance(value, str):
            original = value
            value = value.strip().lower()
            if value:
                if 'true'.startswith(value):
                    return True
                if 'false'.startswith(value):
                    return False
            raise exc.InvalidTextRepresentationError(
                f"invalid input syntax for type boolean: {original!r}"
            )
        if isinstance(value, int):
            return bool(value)
        raise NotImplementedError(type(value))


    def pg_text(value):
        if value is None or isinstance(value, str):
            return value
        if value in (True, False):
            return str(value).lower()
        return str(value)


    return Schema(
        functions={'length': Function(fn=len)},
        types={
            'bool': PgType(
                converter=pg_bool,
                description="boolean, 'true'/'false'",
            ),
            'integer': integer,
            'int4': integer,
            'text': PgType(
                converter=pg_text,
                description="variable-length string, no limit specified",
            ),
        },
    )


@attr.s(frozen=True, slots=True)
class Database:
    schemas = attr.ib(
        converter=frozendict,
        default={
            'public': Schema(),
            'pg_catalog': create_pg_catalog(),
        },
    )

    def _get_table(self, relname, schema_name=None):
        if schema_name is None:
            # TODO: a real search path
            search_path = ['public']
            for schema_name in search_path:
                schema = self.schemas[schema_name]
                if relname in schema.tables:
                    return schema.tables[relname]
            raise exc.UndefinedTableError(table=relname)
        try:
            return self.schemas[schema_name].tables[relname]
        except KeyError:
            raise exc.UndefinedTableError(table=f"{schema_name}.{relname}") from None

    def create_table(self, table):
        return self._update_table(table)

    def update_table(self, table):
        return self._update_table(table)

    def _update_table(self, table):
        schemas = dict(self.schemas)
        schema = schemas.get(table.schema)
        # TODO: this should be an error, but danged if it doesn't make testing easier.
        if not schema:
            schema = Schema()
        tables = dict(schema.tables)
        tables[table.relname] = table
        schemas[table.schema] = attr.evolve(schema, tables=tables)
        return Database(schemas=schemas)

    def parse_select_expr(self, expr, sources=None):
        expr_type = type(expr).__name__
        expr_method = {
            'AConst': lambda expr, sources: Constant(expr.val.val),
            'ColumnRef': self._parse_select_column_ref,
            'AExpr': self._parse_select_aexpr,
            'TypeCast': self._parse_select_typecast,
            'FuncCall': self._parse_select_funccall,
        }.get(expr_type)
        if not expr_method:
            raise NotImplementedError(expr_type)
        return expr_method(expr=expr, sources=sources)

    def _parse_select_column_ref(self, expr, sources):
        last = expr.fields[-1]
        if isinstance(last, psqlparse.nodes.AStar):
            raise NotImplementedError()
        column = last.str
        column_ref = [piece.str for piece in expr.fields[::-1]]
        column_source = sources.get_column_source(*column_ref)
        return Element(lambda row: row[column_source][column], name=column)

    def _parse_select_aexpr(self, expr, sources):
        left_element = self.parse_select_expr(expr.lexpr, sources)
        right_element = self.parse_select_expr(expr.rexpr, sources)
        operation = _get_aexpr_op(expr.name[0].val)
        return Element(lambda row: operation(left_element.eval(row), right_element.eval(row)))

    def _parse_select_typecast(self, expr, sources):
        verify_implemented(expr, ['arg', 'type_name'])
        verify_implemented(
            expr.type_name,
            ['names'],
            # I don't really understand what typemod is.
            # https://doxygen.postgresql.org/format__type_8c_source.html
            expected_values={'typemod': -1},
        )
        type_name = expr.type_name.names[-1].str
        type_ref = [name.str for name in expr.type_name.names[::-1]]
        pgtype = self._get_type(*type_ref)
        elem = self.parse_select_expr(expr.arg, sources)
        return Element(
            lambda row: pgtype.converter(elem.eval(row)),
            name=type_name,
        )

    def _parse_select_funccall(self, expr, sources):
        func_ref = [piece.str for piece in expr.funcname[::-1]]
        func = self._get_function(*func_ref)
        args = [self.parse_select_expr(arg, sources) for arg in expr.args]
        return Element(
            lambda row: func.fn(*(arg.eval(row) for arg in args)),
            name=expr.funcname[-1].str,
        )

    def _get_schema(self, schema_name):
        if schema_name not in self.schemas:
            raise exc.InvalidSchemaNameError(f"schema {schema_name!r} does not exist")
        return self.schemas[schema_name]

    def _get_function(self, func_name, schema_name=None):
        if schema_name is None:
            display_name = func_name
            schema_name = 'pg_catalog'
        else:
            display_name = f"{schema_name}.{func_name}"

        schema = self._get_schema(schema_name)
        if func_name not in schema.functions:
            raise exc.UndefinedFunctionError(
                f"function {display_name}() does not exist"
            )
        return schema.functions[func_name]

    def _get_type(self, type_name, schema_name=None):
        if schema_name is None:
            display_name = type_name
            schema_name = 'pg_catalog'
        else:
            display_name = f"{schema_name}.{type_name}"
        schema = self._get_schema(schema_name)
        if type_name not in schema.types:
            raise exc.UndefinedObjectError(f'type "{display_name}" does not exist')
        return schema.types[type_name]


@attr.s(slots=True)
class QueryTables:  # XXX bad name
    _aliases = attr.ib(default=(), converter=dict)
    # {relname: {schema: _}}
    _tables = attr.ib(default=(), converter=lambda data: collections.defaultdict(dict, data))

    def _clone(self):
        return attr.evolve(
            self,
            tables=(
                (relname, dict(schemas))
                for relname, schemas in self._tables.items()
            )
        )

    def add(self, *, table, alias=None):
        if alias:
            if alias in self._aliases or alias in self._tables:
                raise exc.DuplicateAliasError(alias)
            self._aliases[alias] = table
        else:
            if table.relname in self._aliases or table.schema in self._tables.get(table.relname, ()):
                raise exc.DuplicateAliasError(table.relname)
            self._tables[table.relname][table.schema] = table

    def all_tables(self):
        for alias, table in self._aliases.items():
            yield table, alias
        for schemas in self._tables.values():
            for table in schemas.values():
                yield table, None

    @classmethod
    def merge(cls, *qts):
        qts = iter(qts)
        first_qt = next(qts)
        new = first_qt._clone()
        for qt in qts:
            for table, alias in qt.all_tables():
                new.add(table=table, alias=alias)
        return new

    def _get_source_by_qualified_table(self, schema_name, table_name):
        schemas = self._tables.get(table_name)
        table = schemas and schemas.get(schema_name)
        if not table:
            raise exc.UndefinedTableError(f"missing FROM-clause entry for table {table_name!r}")
        return table, None

    def _get_source_by_table(self, name):
        if name in self._aliases:
            return self._aliases[name], name
        if name in self._tables:
            tables = self._tables[name]
            if len(tables) > 1:
                raise exc.AmbiguousTableError(f"table reference {name!r} is ambiguous")
            return one(tables.values()), None
        raise exc.UndefinedTableError(f"missing FROM-clause entry for table {name!r}")

    def _get_source_by_column(self, column_name):
        column_source = None
        for table, alias in self.all_tables():
            if column_name in table.rowtype.columns:
                if column_source is not None:
                    raise exc.AmbiguousColumnError(f"column reference {column_name!r} is ambiguous")
                column_source = table, alias
        if not column_source:
            raise exc.UndefinedColumnError(f"column {column_name!r} does not exist")
        return column_source

    def get_column_source(self, column_name, table_name=None, schema_name=None):
        if not table_name:
            return self._get_source_by_column(column_name)
        elif not schema_name:
            return self._get_source_by_table(table_name)
        return self._get_source_by_qualified_table(schema_name, table_name)

    def null_row(self):
        return {
            (table, alias): table.rowtype.null_row()
            for table, alias in self.all_tables()
        }


class MockDatabase:
    def __init__(self):
        self._db = Database()

    def _execute_statement(self, statement):
        stmt_type = type(statement).__name__
        handler = QUERY_HANDLERS.get(stmt_type)
        if not handler:
            raise NotImplementedError(stmt_type)
        return handler(self, statement)

    def execute_one(self, query):
        statements = psqlparse.parse(query)
        if len(statements) != 1:
            raise ValueError("multiple statements passed")
        statement, = statements
        return self._execute_statement(statement)

    def execute(self, query):
        return list(self.execute_lazy(query))

    def execute_lazy(self, query):
        statements = psqlparse.parse(query)
        for statement in statements:
            yield self._execute_statement(statement)

    def _handle_create_statement(self, statement):
        relation_data = statement.relation
        column_data = statement.table_elts or []

        table = Table(
            schema=relation_data.schemaname if relation_data.schemaname is not None else 'public',
            relname=relation_data.relname,
            rowtype=Table.generate_rowtype(column_data),
        )
        self._db = self._db.create_table(table)

    def _handle_insert_statement(self, statement):
        relation_data = statement.relation
        col_names = [
            col.name
            for col in statement.cols
        ]
        rows = simple_select(statement.select_stmt, self._db)

        table = self._db._get_table(
            schema_name=relation_data.schemaname,
            relname=relation_data.relname,
        )
        table = table.insert(
            table.rowtype(zip(
                col_names,
                row,
            ))
            for row in rows
        )
        self._db = self._db.update_table(table)

    def _handle_select_statement(self, statement):
        verify_implemented(
            statement,
            ['from_clause', 'target_list', 'where_clause', 'sort_clause'],
            # Not sure what op is.
            expected_values={'op': 0, 'statement': 'SELECT'},
        )
        from_sources = QueryTables()
        rows = [{}]
        for clause in statement.from_clause or ():
            agg_from_source, agg_row = self._parse_from_clauses(clause)
            from_sources = QueryTables.merge(agg_from_source, from_sources)
            rows = self._merge_rows(rows, agg_row)

        # select columns
        row_sources = []
        row_names = []
        for target in statement.target_list or []:
            element = self._db.parse_select_expr(target.val, sources=from_sources)
            name = target.name or ('?column?' if element.name is None else element.name)
            row_sources.append(element)
            row_names.append(name)

        if statement.where_clause:
            where_expr = self._db.parse_select_expr(statement.where_clause, sources=from_sources)
            rows = (
                row for row in rows
                if where_expr.eval(row)
            )

        if statement.sort_clause:
            rows = self._apply_sort_expr(
                sort_clause=statement.sort_clause,
                rows=rows,
                sources=from_sources,
            )

        result_rows = (
            tuple(
                source.eval(row)
                for source in row_sources
            )
            for row in rows
        )

        return ResultSet(
            row_names=row_names,
            rows=result_rows,
        )

    def _apply_sort_expr(self, *, sort_clause, rows, sources):
        strats = [
            (
                SortByStrategy(
                    sortby_dir=expr.sortby_dir,
                    sortby_nulls=expr.sortby_nulls,
                ),
                self._get_sortby_element(expr.node, sources=sources),
            )
            for expr in sort_clause
        ]

        return sorted(rows, key=lambda row: tuple(
            SortByKey(
                strat=strat,
                value=element.eval(row),
            )
            for strat, element in strats
        ))

    def _get_sortby_element(self, expr, sources):
        expr_type = type(expr).__name__
        if expr_type == 'AConst':
            const_type = type(expr.val).__name__
            if const_type != 'Integer':
                raise exc.PostgresSyntaxError("non-integer constant in ORDER BY")
            raise NotImplementedError(expr_type)
        elif expr_type == 'ColumnRef':
            last = expr.fields[-1]
            # I think the parser catches this as a syntax error.
            assert not isinstance(last, psqlparse.nodes.AStar)
            column = last.str
            column_ref = [piece.str for piece in expr.fields[::-1]]
            column_source = sources.get_column_source(*column_ref)
            return Element(lambda row: row[column_source][column], name=column)
        else:
            raise NotImplementedError(expr_type)

    def _merge_rows(self, left_rows, right_rows):
        # TODO: basic join strategies?
        right_rows = list(right_rows)
        return (
            frozendict({**left_row, **right_row})
            for left_row in left_rows
            for right_row in right_rows
        )

    def _inner_merge_rows(self, left_rows, right_rows, quals_expr, left_sources, right_sources):
        del left_sources, right_sources
        rows = self._merge_rows(left_rows, right_rows)
        if quals_expr:
            rows = filter(quals_expr.eval, rows)
        return rows

    def _left_merge_rows(self, left_rows, right_rows, quals_expr, left_sources, right_sources):
        del left_sources
        right_rows = list(right_rows)
        for left_row in left_rows:
            lrow_used = False
            for right_row in right_rows:
                new_row = {**left_row, **right_row}
                if quals_expr.eval(new_row):
                    lrow_used = True
                    yield new_row
            if not lrow_used:
                yield {**left_row, **right_sources.null_row()}

    def _right_merge_rows(self, left_rows, right_rows, quals_expr, left_sources, right_sources):
        # Sneaky. Swap left and right.
        return self._left_merge_rows(
            left_rows=right_rows,
            right_rows=left_rows,
            left_sources=right_sources,
            right_sources=left_sources,
            quals_expr=quals_expr,
        )

    def _full_merge_rows(self, left_rows, right_rows, quals_expr, left_sources, right_sources):
        right_rows = list(right_rows)
        missing_right = set(right_rows)
        for left_row in left_rows:
            lrow_used = False
            for right_row in right_rows:
                new_row = {**left_row, **right_row}
                if quals_expr.eval(new_row):
                    missing_right.discard(right_row)
                    lrow_used = True
                    yield new_row
            if not lrow_used:
                yield {**left_row, **right_sources.null_row()}
        for right_row in missing_right:
            yield {**left_sources.null_row(), **right_row}

    def _merge_clauses(self, clause):
        left_sources, left_rows = self._parse_from_clauses(clause.larg)
        right_sources, right_rows = self._parse_from_clauses(clause.rarg)
        sources = QueryTables.merge(left_sources, right_sources)

        if not clause.quals:  # cross join
            assert clause.jointype == 0, clause.jointype  # i think you can only inner cross-join
            return sources, self._merge_rows(left_rows, right_rows)
        quals_expr = self._db.parse_select_expr(clause.quals, sources=sources)

        join_fn = {
            0: self._inner_merge_rows,
            1: self._left_merge_rows,
            2: self._full_merge_rows,
            3: self._right_merge_rows,
        }[clause.jointype]

        rows = map(frozendict, join_fn(
            left_rows=left_rows,
            right_rows=right_rows,
            quals_expr=quals_expr,
            left_sources=left_sources,
            right_sources=right_sources,
        ))
        return sources, rows

    def _parse_from_clauses(self, clause):
        if isinstance(clause, psqlparse.nodes.RangeVar):
            from_source = QueryTables()
            table = self._db._get_table(clause.relname, schema_name=clause.schemaname)
            alias = clause.alias.aliasname if clause.alias else None
            from_source.add(table=table, alias=alias)
            rows = (frozendict({(table, alias): row}) for row in table.rows)
            return from_source, rows
        elif isinstance(clause, psqlparse.nodes.JoinExpr):
            verify_implemented(clause, ['larg', 'rarg', 'quals', 'jointype'])
            return self._merge_clauses(clause)
        else:
            raise NotImplementedError(type(clause))


def _debug(prefix, obj):
    v = dict(public_fields(obj))
    print(prefix, type(obj), obj, v)
    print()


def simple_select(select_stmt, db):
    # XXX: almost certainly gonna need to rethink how this works.
    values = select_stmt.values_lists
    if values:
        for row in values:
            yield tuple(
                db.parse_select_expr(elem).value for elem in row
            )
    else:
        raise NotImplementedError


@apply(''.join)
def _like_pattern_to_regex(pattern):
    escaped = False
    for char in pattern:
        if escaped:
            if re.fullmatch(r"\W", char):
                yield '\\'
            yield char
            escaped = False
        else:
            if char == '\\':
                escaped = True
            elif char == '_':
                yield '.'
            elif char == '%':
                yield '.*'
            else:
                if re.fullmatch(r"\W", char):
                    yield '\\'
                yield char
    if escaped:
        raise exc.InvalidEscapeSequence("LIKE pattern must not end with escape character")


def like_operator(text, pattern):
    regex = _like_pattern_to_regex(pattern)
    return bool(re.fullmatch(regex, text))


def ilike_operator(text, pattern):
    regex = _like_pattern_to_regex(pattern)
    return bool(re.fullmatch(regex, text, re.IGNORECASE))


@apply
def not_(value):
    """Decorator that boolean-negates the result of a function."""
    return not value


def _get_aexpr_op(symbol):
    operators = {
        '=': operator.__eq__,
        '<>': operator.__ne__,
        '!=': operator.__ne__,
        '>': operator.__gt__,
        '>=': operator.__ge__,
        '<': operator.__lt__,
        '<=': operator.__le__,
        '+': operator.__add__,
        '-': operator.__sub__,
        '~~': like_operator,
        '~~*': ilike_operator,
        '!~~': not_(like_operator),
        '!~~*': not_(ilike_operator),
    }
    if symbol not in operators:
        raise NotImplementedError(symbol)
    return operators[symbol]


QUERY_HANDLERS = {
    'CreateStmt': MockDatabase._handle_create_statement,
    'InsertStmt': MockDatabase._handle_insert_statement,
    'SelectStmt': MockDatabase._handle_select_statement,
}


# ---


def _print_result(result):
    if result is None:
        return
    PADDING = 1
    if result.row_names:
        column_widths = [
            max(len(str(elem)) for elem in column)
            for column in zip(*([result.row_names] + result.rows))
        ]
        print('|'.join(
            f'{name:^{width + PADDING * 2}}'
            for width, name in zip(column_widths, result.row_names)
        ))
        print('+'.join('-' * (width + PADDING * 2) for width in column_widths))
        for row in result.rows:
            print('|'.join(
                f'{" " * PADDING}{elem:{_align(elem)}{width}}{" " * PADDING}'
                for width, elem in zip(column_widths, row)
            ))
    else:
        print('--')
    print(f"({len(result.rows)} row{'' if len(result.rows) == 1 else 's'})")
    print()


def _align(elem):
    return '>' if isinstance(elem, numbers.Number) else '<'


def repl():
    import readline

    db = MockDatabase()
    try:
        while True:
            try:
                query = input('# ')
                for result in db.execute_lazy(query):
                    _print_result(result)
            except KeyboardInterrupt:
                print()
            except EOFError:
                print()
                raise
            except exc.PostgresError as postgres_exc:
                print("ERROR: ", postgres_exc)
            except psqlparse.exceptions.PSqlParseError as psql_exc:
                print("ERROR: ", psql_exc)
                print("LINE 1:", query)
                print("     ", " " * psql_exc.cursorpos, "^")
            # Catch everything and print it instead of crashing the repl.
            except Exception:  # pylint: disable=broad-except
                traceback.print_exc()
    except EOFError:
        pass


if __name__ == '__main__':
    repl()
