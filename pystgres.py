import numbers
import traceback
import typing

import attr
import frozendict
import psqlparse


def dict_one(dict_):
    """
    Fetch the single key and value in the given dict.

    If the dict has not exactly one element, ValueError is raised instead.
    """
    (key_value,) = dict_.items()
    return key_value


@attr.s(slots=True, frozen=True)
class Table:
    schema = attr.ib()
    relname = attr.ib()
    rowtype = attr.ib()
    rows = attr.ib(default=())

    def insert(self, rows):
        # TODO: constraints
        return Table(
            schema=self.schema,
            relname=self.relname,
            rowtype=self.rowtype,
            rows=self.rows + tuple(rows),
        )

    @classmethod
    def generate_rowtype(cls):
        # XXX: obvi
        @attr.s(slots=True, frozen=True)
        class Row:
            baz = attr.ib()
            bang = attr.ib()

        return Row


@attr.s(slots=True, frozen=True)
class ResultSet:
    row_names = attr.ib()
    rows = attr.ib()


class Element(typing.NamedTuple):
    value: typing.Any
    name: str = None


class NoSuchRelationError(Exception):
    """Relation does not exist."""


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
            raise NoSuchRelationError(relname)
        try:
            return self.schemas[schema][relname]
        except KeyError:
            raise NoSuchRelationError(f"{schema}.{relname}") from None

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

    def execute(self, query):
        return list(self.execute_lazy(query))

    def execute_lazy(self, query):
        statements = psqlparse.parse(query)
        for statement in statements:
            handler = QUERY_HANDLERS.get(statement.type)
            if not handler:
                raise NotImplementedError(statement.type)
            yield handler(self, statement)

    def _handle_create_statement(self, statement):
        obj = statement._obj
        relation_data = obj['relation']['RangeVar']
        column_data = obj['tableElts']

        table = Table(
            schema=relation_data['schemaname'],
            relname=relation_data['relname'],
            rowtype=Table.generate_rowtype(
                # XXX
            ),
        )
        self._db = self._db.create_table(table)

    def _handle_insert_statement(self, statement):
        obj = statement._obj
        relation_data = obj['relation']['RangeVar']
        col_names = [
            col['ResTarget']['name']
            for col in obj['cols']
        ]
        rows = simple_select(obj['selectStmt']['SelectStmt'])

        table = self._db._get_table(
            schema=relation_data['schemaname'],
            relname=relation_data['relname'],
        )
        table = table.insert(
            table.rowtype(**dict(zip(
                col_names,
                row,
            )))
            for row in rows
        )
        self._db = self._db.update_table(table)

    def _handle_select_statement(self, statement):
        clause = next(iter(statement.from_clause.items), None)
        if not isinstance(clause, psqlparse.nodes.RangeVar):
            raise NotImplementedError(type(clause))

        table = self._db._get_table(clause.relname, schema=clause.schemaname)

        row_sources = []
        row_names = []
        for target in statement.target_list.targets:
            source, name = parse_select_expr(target['val'], sources=[table])
            name = target.get('name', '?column?' if name is None else name)
            row_sources.append(source)
            row_names.append(name)

        result_rows = [
            [
                _filter_row(source, row)
                for source in row_sources
            ]
            for row in table.rows
        ]

        return ResultSet(
            row_names=row_names,
            rows=result_rows,
        )
        # for clause in statement.from_clause.items:
        #     if isinstance(clause, psqlparse.nodes.RangeVar):
        #         print("range", clause)
        #     else:
        #         print("notrange", clause)


        # _debug(statement)
        # _debug(statement.from_clause)
        # _debug(statement.from_clause.items[0])
        # _debug(statement.target_list.targets)


def _filter_row(source, row):
    try:
        return source(row)
    except TypeError:
        return source


def _debug(prefix, obj):
    v = dir(obj)
    # v.pop('_obj', None)
    print(prefix, type(obj), obj, v)
    print()


def simple_select(select_stmt):
    # XXX: almost certainly gonna need to rethink how this works.
    if 'valuesLists' in select_stmt:
        values = select_stmt['valuesLists']
        for row in values:
            yield tuple(
                parse_select_expr(elem).value for elem in row
            )
    else:
        raise NotImplementedError


def parse_select_expr(expr, sources=None):
    expr_type, data = dict_one(expr)
    if expr_type == 'A_Const':
        const_type, value_data = dict_one(data['val'])
        if const_type == 'Integer':
            return Element(value_data['ival'])
        elif const_type == 'String':
            return Element(value_data['str'])
        else:
            raise NotImplementedError(const_type)
    elif expr_type == 'ColumnRef':
        # TODO: routing
        column = data['fields'][0]['String']['str']
        return Element(lambda row: getattr(row, column), name=column)
    else:
        raise NotImplementedError(expr_type)


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
        INSERT INTO foo.bar (baz, bang) VALUES (1, 'hi'), (1, 'hello');
    """)

    # ---

    db.execute("""
        SELECT 10 as zoom, bang FROM foo.bar;
    """)
    1 / 0


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
