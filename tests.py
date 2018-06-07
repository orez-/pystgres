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


def test_nonsense():
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

    with pytest.raises(exc.AmbiguousTableError):
        db.execute_one("""
            SELECT bar.zam FROM foo.bar JOIN bow.bar ON true;
        """)


def test_ambiguous_tables():
    db = pystgres.MockDatabase()
    db.execute("""
        CREATE TABLE foo.bar (
            baz BIGINT,
            bang TEXT
        );
    """)

    with pytest.raises(exc.DuplicateAliasError):
        db.execute_one("select 1 from foo.bar join foo.bar on true;")


@pytest.mark.parametrize('query', [
    "SELECT 1 FROM foo.bar JOIN bow.bar ON true;",  # different schema
    "SELECT 1 FROM foo.bar JOIN foo.bar _ ON true;",  # aliased
])
def test_not_ambiguous_tables(query):
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
