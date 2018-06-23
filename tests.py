import pytest

import exc
import pystgres


def scalars(iterable):
    return [elem for elem, in iterable]


def test_create_table():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGSERIAL PRIMARY KEY,
            bang TEXT
        );

        CREATE TABLE foo.bam (
            boom BIGSERIAL PRIMARY KEY,
            bing TEXT
        );

        CREATE TABLE zam.zang (
            zoom BIGSERIAL PRIMARY KEY,
            zippy TEXT,
            zloop TEXT
        );
    """)


def test_insert():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGSERIAL PRIMARY KEY,
            bang TEXT
        );
    """)
    db.execute("""
        INSERT INTO foo.bar (baz, bang) VALUES (1, 'hi'), (1, 'hello');
    """)


@pytest.mark.xfail
def test_insert_defaults():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGSERIAL PRIMARY KEY,
            bang TEXT
        );
    """)
    db.execute("""
        INSERT INTO foo.bar (bang) VALUES ('hi'), ('hello');
    """)
    result = db.execute_one("SELECT * FROM foo.bar;")
    assert result.rows == [
        [1, 'hi'],
        [2, 'hello'],
    ]


def test_simple_select():
    db = pystgres.MockDatabase()
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
    db = pystgres.MockDatabase()
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


def test_length_fn():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        INSERT INTO foo.bar (baz, bang) VALUES (1, 'ab'), (2, 'bec'), (3, 'ked');
    """)

    result = db.execute_one("""
        SELECT baz, length(bang) FROM foo.bar;
    """)
    assert result.rows == [
        [1, 2],
        [2, 3],
        [3, 3],
    ]


@pytest.mark.xfail
def test_bare_star():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        INSERT INTO foo.bar (baz, bang) VALUES (1, 'ab'), (2, 'bec'), (3, 'ked');
    """)

    result = db.execute_one("""
        SELECT * FROM foo.bar a JOIN foo.bar b ON a.baz = length(b.bang);
    """)
    assert result.rows == [
        [2, 'bec', 1, 'ab'],
        [3, 'ked', 2, 'bec'],
        [3, 'ked', 3, 'ked'],
    ]


@pytest.mark.xfail
def test_qualified_star():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        CREATE TABLE foo.zap (
            id BIGINT,
            data TEXT
        );

        INSERT INTO foo.bar (baz, bang) VALUES (1, 'one'), (2, 'two'), (3, 'three');
        INSERT INTO foo.zap (id, data) VALUES (1, 'huh'), (2, 'neat'), (3, 'cool');
    """)

    results = db.execute_one("""
        SELECT zap.data, bar.*, 'wow' wow FROM foo.bar JOIN foo.zap ON baz = id;
    """)
    assert results.row_names == ['data', 'baz', 'bang', 'wow']
    assert results.rows == [
        ['huh', 1, 'one', 'wow'],
        ['neat', 2, 'two', 'wow'],
        ['cool', 3, 'three', 'wow'],
    ]


def test_ambiguous_column():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        CREATE TABLE bow.bar (
            baz BIGINT,
            zam TEXT
        );
    """)

    with pytest.raises(exc.AmbiguousColumnError):
        db.execute_one("""
            SELECT baz FROM foo.bar JOIN bow.bar ON true;
        """)


def test_explicit_table():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        CREATE TABLE bow.bom (
            baz BIGINT,
            zam TEXT
        );

        INSERT INTO foo.bar (baz, bang) VALUES (1, 'one');
        INSERT INTO bow.bom (baz, zam) VALUES (2, 'neat');
    """)
    result = db.execute_one("""
        SELECT bar.baz FROM foo.bar JOIN bow.bom ON true;
    """)
    assert result.rows == [[1]]


def test_explicit_schema():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        CREATE TABLE bow.bar (
            baz BIGINT,
            zam TEXT
        );

        INSERT INTO foo.bar (baz, bang) VALUES (10, 'ten');
        INSERT INTO bow.bar (baz, zam) VALUES (11, 'cool');
    """)
    result = db.execute_one("""
        SELECT foo.bar.baz FROM foo.bar JOIN bow.bar ON true;
    """)
    assert result.rows == [[10]]


def test_duplicate_aliases():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );
    """)

    with pytest.raises(exc.DuplicateAliasError):
        db.execute_one("SELECT 1 FROM foo.bar JOIN foo.bar ON true;")


@pytest.mark.parametrize('query', [
    "SELECT 1 FROM foo.bar JOIN bow.bar ON true;",  # different schema
    "SELECT 1 FROM foo.bar JOIN foo.bar _ ON true;",  # aliased
])
def test_not_duplicate_aliases(query):
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        CREATE TABLE bow.bar (
            baz BIGINT,
            zam TEXT
        );
    """)

    db.execute_one(query)


@pytest.mark.parametrize("expression,expected", [
    ('%a%', [['a'], ['ab'], ['za']]),
    ('a%', [['a'], ['ab']]),
    ('%a', [['a'], ['za']]),
    ('a', [['a']]),
    (r'\%%', [['%oops']]),
])
def test_like_operator(expression, expected):
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        INSERT INTO foo.bar (baz, bang)
        VALUES (1, 'a'), (1, 'b'), (2, 'ab'), (2, 'za'), (3, 'wow'), (1, 'c'), (3, 'huh'),
        (5, '%oops'), (6, 'ALRIGHT');
    """)
    result = db.execute_one(f"SELECT bang FROM foo.bar WHERE bang LIKE '{expression}';")
    assert result.rows == expected


@pytest.mark.parametrize("expression,expected", [
    ('%a%', [['a'], ['ab'], ['za'], ['ALRIGHT']]),
    ('a%', [['a'], ['ab'], ['ALRIGHT']]),
    ('%a', [['a'], ['za']]),
    ('a', [['a']]),
    (r'\%%', [['%oops']]),
])
def test_ilike_operator(expression, expected):
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        INSERT INTO foo.bar (baz, bang)
        VALUES (1, 'a'), (1, 'b'), (2, 'ab'), (2, 'za'), (3, 'wow'), (1, 'c'), (3, 'huh'),
        (5, '%oops'), (6, 'ALRIGHT');
    """)
    result = db.execute_one(f"SELECT bang FROM foo.bar WHERE bang ILIKE '{expression}';")
    assert result.rows == expected


@pytest.mark.xfail
@pytest.mark.parametrize("column,expected", [
    ('bang', [[1], [2], [3]]),
    ('array_agg(bang)', [[('a', 'b', 'c')], [('ab', 'za')], [('wow', 'huh')]]),
])
def test_aggregation(column, expected):
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        INSERT INTO foo.bar (baz, bang)
        VALUES (1, 'a'), (1, 'b'), (2, 'ab'), (2, 'za'), (3, 'wow'), (1, 'c'), (3, 'huh');
    """)

    result = db.execute_one(f"SELECT {column} FROM foo.bar GROUP BY baz;")
    assert result.rows == expected


@pytest.mark.xfail
@pytest.mark.parametrize('baz, group_by', [
    ('baz', '1'),  # ordinal
    ('baz as zow', 'zow'),  # output column
    ('baz as zow', 'baz'),  # input column
])
def test_group_by_exprs(baz, group_by):
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        INSERT INTO foo.bar (baz, bang)
        VALUES (1, 'a'), (1, 'b'), (2, 'ab'), (2, 'za'), (3, 'wow'), (1, 'c'), (3, 'huh');
    """)
    result = db.execute(f"SELECT {baz}, count(*) FROM foo.bar GROUP BY {group_by};")
    assert result.rows == [[1, 3], [2, 2], [3, 2]]


@pytest.mark.xfail
def test_group_by_ambiguous():
    """
    Ensure input-columns have precedence over output-column names.

    https://www.postgresql.org/docs/current/static/sql-select.html#SQL-GROUPBY
    """
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        INSERT INTO foo.bar (baz, bang)
        VALUES (1, 'a'), (1, 'b'), (2, 'ab'), (2, 'za'), (3, 'wow'), (1, 'c'), (3, 'huh');
    """)
    result = db.execute("SELECT baz as zow, 1 as baz FROM foo.bar GROUP BY baz;")
    assert result.rows == [[1, 1], [2, 1], [3, 1]]


@pytest.mark.xfail
def test_group_by_multi():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            a BIGINT,
            b TEXT,
            c TEXT,
            d TEXT
        );
        INSERT INTO foo.bar (a, b, c, d) VALUES
        (1, 'hey', 'neat', 'cool'),
        (1, 'hey', 'neat', 'rad'),
        (2, 'hey', 'neat', 'awesome'),
        (1, 'hey', 'alright', 'ok'),
        (1, 'oh', 'neat', 'zounds');
    """)
    result = db.execute("SELECT a, b, c, count(*) FROM foo.alpha GROUP BY a, b, c;")
    assert result.rows == [
        [1, 'hey', 'neat', 2],
        [2, 'hey', 'neat', 1],
        [1, 'hey', 'alright', 1],
        [1, 'oh', 'neat', 1],
    ]


@pytest.mark.xfail
def test_group_by_expression():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );

        INSERT INTO foo.bar (baz, bang)
        VALUES (1, 'a'), (1, 'b'), (2, 'ab'), (2, 'za'), (3, 'wow'), (1, 'c'), (3, 'huh');
    """)
    result = db.execute("SELECT bang LIKE '%a%', count(*) FROM foo.bar GROUP BY bang LIKE '%a%';")
    assert result.rows == [
        [True, 3],
        [False, 4],
    ]


def test_simple_where_clause():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );
        INSERT INTO foo.bar (baz, bang) VALUES (10, 'ten'), (11, 'eleven'), (12, 'twelve');
    """)

    result = db.execute_one("SELECT baz, bang FROM foo.bar WHERE baz = 11;")
    assert result.rows == [[11, 'eleven']]


def test_no_from():
    db = pystgres.MockDatabase()
    result = db.execute_one("SELECT 1, 'wow';")
    assert result.rows == [[1, 'wow']]


def test_empty_select():
    db = pystgres.MockDatabase()
    result = db.execute_one("SELECT;")
    assert result.rows == [[]]


def test_comma_join():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );
        CREATE TABLE foo.bam (
            bing BIGINT,
            zoop TEXT
        );
        INSERT INTO foo.bar (baz, bang) VALUES (10, 'ten'), (11, 'eleven'), (12, 'twelve');
        INSERT INTO foo.bam (bing, zoop) VALUES (1, 'zip'), (2, 'zap'), (3, 'zam');
    """)
    result = db.execute_one("SELECT bang, zoop FROM foo.bar, foo.bam;")
    assert result.rows == [
        ['ten', 'zip'],
        ['ten', 'zap'],
        ['ten', 'zam'],
        ['eleven', 'zip'],
        ['eleven', 'zap'],
        ['eleven', 'zam'],
        ['twelve', 'zip'],
        ['twelve', 'zap'],
        ['twelve', 'zam'],
    ]


@pytest.mark.parametrize('expr,expected', [
    ("'30'::INT", 30),
    ("'t'::BOOL", True),
    ("'tru'::BOOL", True),
    ("'  true '::BOOL", True),
    ("' fals '::BOOL", False),
    ("tRuE::TEXT", 'true'),
    ("30::text", '30'),
])
def test_cast(expr, expected):
    db = pystgres.MockDatabase()
    result = db.execute_one(f"SELECT {expr}")
    assert scalars(result.rows) == [expected]


@pytest.mark.xfail
def test_setof_type():
    db = pystgres.MockDatabase()
    result = db.execute_one("""
        SELECT regexp_matches('4 8 15 16 23 42', '\d+', 'g');
    """)
    assert result.rows == [
        [('4',)],
        [('8',)],
        [('15',)],
        [('16',)],
        [('23',)],
        [('42',)],
    ]


@pytest.mark.xfail
def test_double_setof():
    result = db.execute_one("""
        SELECT unnest(regexp_matches('2 5 11 23 47 95', '\d+', 'g'))::int;
    """)
    assert scalars(result.rows) == [2, 5, 11, 23, 47, 95]


@pytest.mark.parametrize('order_by,expected', [
    ('bang, boom, bop', [2, 1, 4, 5, 3, 6, 8, 7]),
    ('bang desc, boom desc, bop desc', [7, 8, 6, 3, 5, 4, 1, 2]),
    ('bang asc, boom desc, bop', [5, 4, 2, 1, 8, 7, 3, 6]),
    ('boom, bang desc, bop', [3, 6, 2, 1, 8, 7, 4, 5]),
    ('bop, bang, boom', [2, 8, 1, 4, 5, 3, 7, 6]),
    ('boom asc, bang, bop', [2, 1, 3, 6, 4, 8, 7, 5]),
    ('boom desc, bang, bop', [5, 4, 8, 7, 2, 1, 3, 6]),
    ('boom NULLS FIRST, bang, bop', [5, 2, 1, 3, 6, 4, 8, 7]),
    ('boom NULLS LAST, bang, bop', [2, 1, 3, 6, 4, 8, 7, 5]),
    ('boom ASC NULLS FIRST, bang, bop', [5, 2, 1, 3, 6, 4, 8, 7]),
    ('boom ASC NULLS LAST, bang, bop', [2, 1, 3, 6, 4, 8, 7, 5]),
    ('boom DESC NULLS FIRST, bang, bop', [5, 4, 8, 7, 2, 1, 3, 6]),
    ('boom DESC NULLS LAST, bang, bop', [4, 8, 7, 2, 1, 3, 6, 5]),
])
def test_order_by(order_by, expected):
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.zap (
            baz BIGINT,
            bang TEXT,
            boom TEXT,
            bop BIGINT
        );
        INSERT INTO foo.zap (baz, bang, boom, bop) VALUES
        (1, 'one', 'hera', 20),
        (2, 'one', 'hera', 10),
        (3, 'two', 'hera', 20),
        (4, 'one', 'hermes', 20),
        (5, 'one', NULL, 20),
        (6, 'two', 'hera', 30),
        (7, 'two', 'hermes', 20),
        (8, 'two', 'hermes', 10);
    """)
    result = db.execute_one(f"""
        SELECT baz FROM foo.zap ORDER BY {order_by};
    """)
    assert scalars(result.rows) == expected
