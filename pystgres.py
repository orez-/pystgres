from __future__ import generator_stop

import numbers
import operator
import traceback
import typing

import attr
import frozendict
import psqlparse

import exc


def first(iterable):
    return next(iter(iterable))


def one(iterable):
    only, = iterable
    return only


class AbstractRow(frozendict.frozendict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._address_missing()
        self._address_extra()

    def _address_missing(self):
        """
        Fill defaults of missing columns, or raise if a missing column has no default.
        """
        for column in type(self).columns:
            if column not in self:
                raise exc.IntegrityConstraintViolation(
                    f"null value in column {column!r} violates not-null constraint\n"
                    f"Failing row contains ({', '.join(self.values())})."
                )

    def _address_extra(self):
        extra = set(self) - set(type(self).columns)
        if extra:
            raise exc.IntegrityConstraintViolation(
                f"column {first(extra)} of relation \"?\" does not exist"
            )

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
    rows = attr.ib()


class Element(typing.NamedTuple):
    value: typing.Any
    name: str = None

    def eval(self, row):
        try:
            return self.value(row)
        except TypeError:
            return self.value


@attr.s(frozen=True, slots=True)
class Database:
    schemas = attr.ib(default=frozendict.frozendict())  # TODO: schema objects

    def _get_table(self, relname, schema=None):
        if schema is None:
            # TODO: a real search path
            search_path = ['public']
            for schema_name in search_path:
                schema = self.schemas[schema_name]
                if relname in schema:
                    return schema[relname]
            raise exc.NoSuchRelationError(relname)
        try:
            return self.schemas[schema][relname]
        except KeyError:
            raise exc.NoSuchRelationError(f"{schema}.{relname}") from None

    def create_table(self, table):
        return self._update_table(table)

    def update_table(self, table):
        return self._update_table(table)

    def _update_table(self, table):
        schemas = dict(self.schemas)
        schema = dict(schemas.get(table.schema, ()))  # TODO: schema objects
        schema[table.relname] = table
        schemas[table.schema] = frozendict.frozendict(schema)
        return Database(
            schemas=frozendict.frozendict(schemas),
        )


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
        column_data = statement.table_elts

        table = Table(
            schema=relation_data.schemaname,
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
        rows = simple_select(statement.select_stmt)

        table = self._db._get_table(
            schema=relation_data.schemaname,
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
        clause = next(iter(statement.from_clause), None)
        from_sources, rows = self._parse_from_clauses(clause)

        # select columns
        row_sources = []
        row_names = []
        for target in statement.target_list:
            element = parse_select_expr(target.val, sources=from_sources)
            name = target.name or ('?column?' if element.name is None else element.name)
            row_sources.append(element)
            row_names.append(name)

        result_rows = [
            [
                source.eval(row)
                for source in row_sources
            ]
            for row in rows
        ]

        return ResultSet(
            row_names=row_names,
            rows=result_rows,
        )

    def _parse_from_clauses(self, clause):
        if isinstance(clause, psqlparse.nodes.RangeVar):
            name = clause.alias.aliasname if clause.alias else clause.relname
            table = self._db._get_table(clause.relname, schema=clause.schemaname)
            from_source = {name: table}
            rows = ({name: row} for row in table.rows)
            return from_source, rows
        elif isinstance(clause, psqlparse.nodes.JoinExpr):
            left_sources, left_rows = self._parse_from_clauses(clause.larg)
            right_sources, right_rows = self._parse_from_clauses(clause.rarg)

            # sources
            dups_names = left_sources.keys() & right_sources.keys()
            if dups_names:
                raise exc.DuplicateAliasError(
                    f"table name {first(dups_names)!r} specified more than once"
                )
            sources = {**left_sources, **right_sources}

            # rows
            # TODO: basic join strategies?
            right_rows = list(right_rows)
            rows = (
                {**left_row, **right_row}
                for left_row in left_rows
                for right_row in right_rows
            )

            if clause.quals:
                quals_expr = parse_select_expr(clause.quals, sources=sources)
                rows = (
                    row for row in rows
                    if quals_expr.eval(row)
                )
            else:
                raise NotImplementedError(clause)

            return sources, rows
        else:
            raise NotImplementedError(type(clause))


def _debug(prefix, obj):
    v = dir(obj)
    # v.pop('_obj', None)
    print(prefix, type(obj), obj, v)
    print()


def simple_select(select_stmt):
    # XXX: almost certainly gonna need to rethink how this works.
    values = select_stmt.values_lists
    if values:
        for row in values:
            yield tuple(
                parse_select_expr(elem).value for elem in row
            )
    else:
        raise NotImplementedError


def parse_select_expr(expr, sources=None):
    expr_type = type(expr).__name__
    if expr_type == 'AConst':
        return Element(expr.val.val)
    elif expr_type == 'ColumnRef':
        column = expr.fields[0].str
        column_source = _get_column_source(column, sources)
        return Element(lambda row: getattr(row[column_source], column), name=column)
    elif expr_type == 'AExpr':
        left_element = parse_select_expr(expr.lexpr, sources)
        right_element = parse_select_expr(expr.rexpr, sources)
        operation = _get_aexpr_op(expr.name[0].val)
        return Element(lambda row: operation(left_element.eval(row), right_element.eval(row)))
    else:
        raise NotImplementedError(expr_type)


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
    }
    if symbol not in operators:
        raise NotImplementedError(symbol)
    return operators[symbol]


def _get_column_source(column_name, sources):
    column_source = None
    for name, table in sources.items():
        if column_name in table.rowtype.columns:
            if column_source is not None:
                raise AmbiguousColumnError(f"column reference {column_name!r} is ambiguous")
            column_source = name
    if not column_source:
        raise exc.UndefinedColumnError(f"column {column_name!r} does not exist")
    return column_source


QUERY_HANDLERS = {
    'CreateStmt': MockDatabase._handle_create_statement,
    'InsertStmt': MockDatabase._handle_insert_statement,
    'SelectStmt': MockDatabase._handle_select_statement,
}


# ---


def test_create_table():
    db = MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGSERIAL PRIMARY KEY,
            bang TEXT
        );
    """)


def test_insert():
    db = MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGSERIAL PRIMARY KEY,
            bang TEXT
        );
    """)
    db.execute("""
        INSERT INTO foo.bar (baz, bang) VALUES (1, 'hi'), (1, 'hello');
    """)


# def test_insert_defaults():
#     db = MockDatabase()
#     db.execute("""
#         CREATE TABLE foo.bar (
#             baz BIGSERIAL PRIMARY KEY,
#             bang TEXT
#         );
#     """)
#     db.execute("""
#         INSERT INTO foo.bar (bang) VALUES ('hi'), ('hello');
#     """)
#     1 / 0


def test_simple_select():
    db = MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGSERIAL PRIMARY KEY,
            bang TEXT
        );
    """)
    db.execute("""
        INSERT INTO foo.bar (baz, bang) VALUES (1, 'hi'), (2, 'hello');
    """)

    # ---

    result = db.execute_one("""
        SELECT 10 as zoom, bang FROM foo.bar;
    """)

    assert result.rows == [[10, 'hi'], [10, 'hello']]


def test_simple_join():
    db = MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT PRIMARY KEY,
            bang TEXT
        );
    """)
    db.execute("""
        CREATE TABLE foo.zow (
            bar_baz BIGINT PRIMARY KEY,
            bam TEXT
        );
    """)

    db.execute("""
        INSERT INTO foo.bar (baz, bang) VALUES (1, 'hi'), (2, 'hello'), (3, 'sup'), (4, 'salutations'), (5, 'yo');
        INSERT INTO foo.zow (bar_baz, bam) VALUES (3, 'three'), (6, 'six!?'), (1, 'one'), (2, 'two'), (4, 'four');
    """)

    result = db.execute_one("""
        SELECT baz, bang, bam FROM foo.bar bob JOIN foo.zow ON baz = bar_baz;
    """)
    assert result.rows == [
        [1, 'hi', 'one'],
        [2, 'hello', 'two'],
        [3, 'sup', 'three'],
        [4, 'salutations', 'four'],
    ]


def _print_result(result):
    if result is None:
        return
    PADDING = 1
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
                for result in db.execute_lazy(input('# ')):
                    _print_result(result)
            except KeyboardInterrupt:
                print()
            except EOFError:
                print()
                raise
            except Exception:
                traceback.print_exc()
    except EOFError:
        pass


if __name__ == '__main__':
    repl()
