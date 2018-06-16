from __future__ import generator_stop

import collections
import numbers
import operator
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


class AbstractRow(frozendict):
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
class Schema:
    tables = attr.ib(default=(), converter=frozendict)
    functions = attr.ib(default=(), converter=frozendict)


@attr.s(frozen=True, slots=True)
class Function:
    fn = attr.ib()


def create_pg_catalog():
    return Schema(functions={'length': Function(fn=len)})


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
        if expr_type == 'AConst':
            return Element(expr.val.val)
        elif expr_type == 'ColumnRef':
            last = expr.fields[-1]
            if isinstance(last, psqlparse.nodes.AStar):
                raise NotImplementedError()
            column = last.str
            column_ref = [piece.str for piece in expr.fields[::-1]]
            column_source = sources.get_column_source(*column_ref)
            return Element(lambda row: row[column_source][column], name=column)
        elif expr_type == 'AExpr':
            left_element = self.parse_select_expr(expr.lexpr, sources)
            right_element = self.parse_select_expr(expr.rexpr, sources)
            operation = _get_aexpr_op(expr.name[0].val)
            return Element(lambda row: operation(left_element.eval(row), right_element.eval(row)))
        elif expr_type == 'TypeCast':
            return self.parse_select_expr(expr.arg, sources)
        elif expr_type == 'FuncCall':
            func_ref = [piece.str for piece in expr.funcname[::-1]]
            func = self._get_function(*func_ref)
            args = [self.parse_select_expr(arg, sources) for arg in expr.args]
            return Element(
                lambda row: func.fn(*(arg.eval(row) for arg in args)),
                name=expr.funcname[-1].str,
            )
        else:
            raise NotImplementedError(expr_type)

    def _get_function(self, func_name, schema_name=None):
        # TODO: so trusting.
        if schema_name is not None:
            return self.schemas[schema_name].functions[func_name]
        return self.schemas['pg_catalog'].functions[func_name]


class QueryTables:  # XXX bad name
    def __init__(self):
        self._aliases = {}
        self._tables = collections.defaultdict(dict)  # {relname: {schema: _}}

    def _clone(self):
        qt = QueryTables()
        qt._aliases = dict(self._aliases)
        qt._tables = collections.defaultdict(
            dict,
            (
                (relname, dict(schemas))
                for relname, schemas in self._tables.items()
            )
        )
        return qt

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

    def _merge_rows(self, left_rows, right_rows):
        # TODO: basic join strategies?
        right_rows = list(right_rows)
        return (
            {**left_row, **right_row}
            for left_row in left_rows
            for right_row in right_rows
        )

    def _merge_clauses(self, left_clause, right_clause):
        left_sources, left_rows = self._parse_from_clauses(left_clause)
        right_sources, right_rows = self._parse_from_clauses(right_clause)

        sources = QueryTables.merge(left_sources, right_sources)
        rows = self._merge_rows(left_rows, right_rows)
        return sources, rows

    def _parse_from_clauses(self, clause):
        if isinstance(clause, psqlparse.nodes.RangeVar):
            from_source = QueryTables()
            table = self._db._get_table(clause.relname, schema_name=clause.schemaname)
            alias = clause.alias.aliasname if clause.alias else None
            from_source.add(table=table, alias=alias)
            rows = ({(table, alias): row} for row in table.rows)
            return from_source, rows
        elif isinstance(clause, psqlparse.nodes.JoinExpr):
            sources, rows = self._merge_clauses(clause.larg, clause.rarg)

            if clause.quals:
                quals_expr = self._db.parse_select_expr(clause.quals, sources=sources)
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
            except Exception:
                traceback.print_exc()
    except EOFError:
        pass


if __name__ == '__main__':
    repl()
