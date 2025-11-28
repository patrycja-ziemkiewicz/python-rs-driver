import pytest
from scylla._rust.session_builder import SessionBuilder  # pyright: ignore[reportMissingModuleSource]

from scylla._rust.row import CqlRow  # pyright: ignore[reportMissingModuleSource]


@pytest.mark.asyncio
async def test_simple_deserialization():
    # 1. Connect
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # 2. Create keyspace & table
    await session.execute("""
        CREATE KEYSPACE IF NOT EXISTS testks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)

    await session.execute("""
                          CREATE TABLE IF NOT EXISTS testks.example_table (
                                                                              id int PRIMARY KEY,
                                                                              value_int int,
                                                                              value_text text
                          );
                          """)

    # 3. Insert test rows
    await session.execute("""
                          INSERT INTO testks.example_table (id, value_int, value_text)
                          VALUES (1, 42, 'hello');
                          """)

    await session.execute("""
                          INSERT INTO testks.example_table (id, value_int, value_text)
                          VALUES (2, 99, 'world');
                          """)

    # 4. Query the data
    result = await session.execute("SELECT * FROM testks.example_table")

    # 5. Print CQL values (should not throw)
    rows = result.create_rows_result()
    for row in rows:
        print(row)


@pytest.mark.asyncio
async def test_list_deserialization():
    # 1. Connect
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # 3. Create a table with complex CQL types
    await session.execute("""
                          CREATE TABLE IF NOT EXISTS testks.complex_table2 (
                                                                              id int PRIMARY KEY,
                                                                              name text,
                                                                              scores list<int>,
                          );
                          """)

    # 4. Insert complex rows
    await session.execute("""
                          INSERT INTO testks.complex_table2
                              (id, name, scores)
                          VALUES (
                                     1,
                                     'Alice',
                                     [10, 20, 30]
                                 );
                          """)

    await session.execute("""
                          INSERT INTO testks.complex_table2
                              (id, name, scores)
                          VALUES (
                                     2,
                                     'Bob',
                                     [100, 200]
                                 );
                          """)

    # 5. Query the data
    result = await session.execute("SELECT * FROM testks.complex_table2")

    rows = result.create_rows_result()

    for row in rows:
        assert isinstance(row, dict)


def test_invalid_row_factory_raises_type_error():
    class NotAFactory:
        def build(self, column: CqlRow):
            pass

    builder = SessionBuilder(["127.0.0.2"], 9042)

    # connect is async
    async def run():
        session = await builder.connect()
        result = await session.execute("SELECT * FROM system.local")

        with pytest.raises(TypeError) as exc:
            result.create_rows_result(NotAFactory())  # <-- wrong type

        assert "RowFactory" in str(exc.value)

    import asyncio

    asyncio.run(run())


@pytest.mark.asyncio
async def test_udt_deserialization():
    # 1. Connect
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # 2. Create UDT + table
    await session.execute("""
        CREATE TYPE IF NOT EXISTS testks.address (
            street text,
            number int
        );
    """)

    await session.execute("""
                          CREATE TABLE IF NOT EXISTS testks.persons_udt (
                                                                            id int PRIMARY KEY,
                                                                            name text,
                                                                            addr address
                          );
                          """)

    # 3. Insert 2 rows with UDT values
    await session.execute("""
                          INSERT INTO testks.persons_udt (id, name, addr)
                          VALUES (
                                     1,
                                     'Alice',
                                     { street: 'Main St', number: 10 }
                                 );
                          """)

    await session.execute("""
                          INSERT INTO testks.persons_udt (id, name, addr)
                          VALUES (
                                     2,
                                     'Bob',
                                     { street: 'Oak Ave', number: 42 }
                                 );
                          """)

    # 4. Query + convert rows using your deserializer
    result = await session.execute("SELECT * FROM testks.persons_udt")
    rows = result.create_rows_result()

    # 5. Verify Python structure
    row_list = list(rows)

    assert isinstance(row_list[0], dict)
    assert isinstance(row_list[1], dict)

    # Check first row
    assert row_list[0]["name"] == "Alice"
    assert row_list[0]["addr"]["street"] == "Main St"
    assert row_list[0]["addr"]["number"] == 10

    # Check second row
    assert row_list[1]["name"] == "Bob"
    assert row_list[1]["addr"]["street"] == "Oak Ave"
    assert row_list[1]["addr"]["number"] == 42


@pytest.mark.asyncio
async def test_list_udt_deserialization():
    # 1. Connect
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # 2. Define UDT + table with list<address>
    await session.execute("""
        CREATE TYPE IF NOT EXISTS testks.address (
            street text,
            number int
        );
    """)

    await session.execute("""
                          CREATE TABLE IF NOT EXISTS testks.people_with_addresses (
                                                                                      id   int,
                                                                                      name text,
                                                                                      addrs list<frozen<address>>,
                                                                                      PRIMARY KEY (id, name)
                              );
                          """)

    # 3. Insert multiple rows with list<udt>
    rows_to_insert = 12  # 10–15 as requested
    for i in range(rows_to_insert):
        await session.execute(f"""
            INSERT INTO testks.people_with_addresses (id, name, addrs)
            VALUES (
                0,
                'User{i}',
                [
                    {{ street: 'A-Street-{i}', number: {i * 10} }},
                    {{ street: 'B-Street-{i}', number: {i * 10 + 1} }}
                ]
            );
        """)

    # 4. Query + deserialize
    result = await session.execute("""
                                   SELECT * FROM testks.people_with_addresses
                                    WHERE id = 0
                                   ORDER BY name ASC
                                   """)
    rows = result.create_rows_result()
    row_list = list(rows)

    # 5. Assertions — verify all rows returned + structure is correct
    assert len(row_list) == rows_to_insert

    for i, row in enumerate(row_list):
        print(row)
        assert isinstance(row["addrs"], list)
