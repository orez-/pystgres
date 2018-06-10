import pytest

import exc
import pystgres


def test_create_table():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGSERIAL PRIMARY KEY,
            bang TEXT
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


# def test_insert_defaults():
#     db = pystgres.MockDatabase()
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


@pytest.mark.xfail
@pytest.mark.parametrize("column,expected", [
    ('bang', [1, 2, 3]),
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
