"""
Database Utils tests
"""

import json
import os
import tempfile
from pathlib import Path

import database.utils as utils
import pandas as pd
import pytest
import sqlalchemy
from database import tapdb
from pandas.testing import assert_frame_equal
from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, Table, inspect

# Global variables
inst_dir = None
tapdb_test = None


def setup_module():
    global tapdb_test, inst_dir
    tapdb_test = tapdb.tapdb_engine(host='localhost')
    inst_dir = inst_dir = Path(__file__).resolve().parent / "inst"


def teardown_module():
    tapdb_test.dispose()


def create_temp_tapdb():
    tempdb = sqlalchemy.create_engine('sqlite:///:memory:')
    utils.copy_table_schema(tapdb_test, tempdb)
    return tempdb


def test_copy_table_schema_sqlite():
    # using memory for input and output
    from_engine = sqlalchemy.create_engine('sqlite:///:memory:')
    metadata = MetaData()
    _ = Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String),
              Column('fullname', String)
              )

    _ = Table('addresses', metadata,
              Column('id', Integer, primary_key=True),
              Column('user_id', None, ForeignKey('users.id')),
              Column('email_address', String, nullable=False)
              )

    metadata.create_all(bind=from_engine)

    test_engine = sqlalchemy.create_engine('sqlite:///:memory:')
    utils.copy_table_schema(from_engine, test_engine)

    actual = MetaData()
    actual.reflect(bind=test_engine)
    assert len(metadata.sorted_tables) == len(actual.sorted_tables)
    assert metadata.tables.keys() == actual.tables.keys()

    # exclude table name
    test_engine = sqlalchemy.create_engine('sqlite:///:memory:')
    utils.copy_table_schema(from_engine, test_engine, exclude_tables=['addresses'])
    actual = MetaData()
    actual.reflect(bind=test_engine)
    assert actual.tables.keys() == {'users'}

    # exclude regex
    test_engine = sqlalchemy.create_engine('sqlite:///:memory:')
    utils.copy_table_schema(from_engine, test_engine, exclude_regex='add*')
    actual = MetaData()
    actual.reflect(bind=test_engine)
    assert actual.tables.keys() == {'users'}

    # exclude both
    test_engine = sqlalchemy.create_engine('sqlite:///:memory:')
    utils.copy_table_schema(from_engine, test_engine, exclude_tables=['users'], exclude_regex='add*')
    actual = MetaData()
    actual.reflect(bind=test_engine)
    assert len(actual.tables) == 0

    # using files for output
    db_name = os.path.join(tempfile.gettempdir(), 'copy_schema_test_from.sqlite')
    test_engine = sqlalchemy.create_engine('sqlite:///' + db_name)
    utils.copy_table_schema(from_engine, test_engine)

    actual = MetaData()
    actual.reflect(bind=test_engine)
    assert len(metadata.sorted_tables) == len(actual.sorted_tables)
    assert metadata.tables.keys() == actual.tables.keys()


def test_copy_table_schema():
    # Use TAPDB as the test
    from_engine = utils.make_engine('tapdb', host='localhost')
    to_engine = utils.make_engine('temp_tapdb', host='temp')

    from_meta = MetaData()
    from_meta.reflect(bind=from_engine)

    # with no exclude tables
    utils.copy_table_schema(from_engine, to_engine)
    to_meta = MetaData()
    to_meta.reflect(bind=to_engine)
    assert len(to_meta.sorted_tables) == len(from_meta.sorted_tables)
    assert to_meta.tables.keys() == from_meta.tables.keys()
    to_engine.dispose()

    # with exclude table that already would have been excluded and another table
    utils.copy_table_schema(from_engine, to_engine, exclude_regex='pos*')
    to_meta = MetaData()
    to_meta.reflect(bind=to_engine)
    assert len(to_meta.sorted_tables) == 2
    assert to_meta.tables.keys() == {'source', 'orders_df'}


def test_copy_table_data():
    source_engine = utils.make_engine('tsdb', host='localhost')
    test_engine = utils.make_engine('temp_tsdb', host='temp')

    # copy schema
    utils.copy_table_schema(source_engine, test_engine, exclude_regex='ts_*|attribute*')

    # using regex
    utils.copy_table_data(source_engine, test_engine, include_regex='data_*')

    result = pd.read_sql_table('data_source', test_engine)
    assert result.columns.tolist() == ['data_source_id', 'data_source_name']
    assert 'test_source_01' in result['data_source_name'].tolist()
    assert 'test_source_02' in result['data_source_name'].tolist()

    result = pd.read_sql_table('data_table', test_engine)
    assert result.columns.tolist() == ['data_table_id', 'data_table_name']
    assert 'ts_test' in result['data_table_name'].tolist()

    result = pd.read_sql_table('source', test_engine)
    assert result.empty

    # include tables
    utils.copy_table_data(source_engine, test_engine, include_tables=['data_source'])

    result = pd.read_sql_table('data_source', test_engine)
    assert result.columns.tolist() == ['data_source_id', 'data_source_name']
    assert 'test_source_01' in result['data_source_name'].tolist()
    assert 'test_source_02' in result['data_source_name'].tolist()

    result = pd.read_sql_table('data_table', test_engine)
    assert result.empty

    # No tables
    utils.copy_table_data(source_engine, test_engine)
    actual = pd.read_sql_table('data_source', test_engine)
    assert actual.empty

    # cannot work cross MySQL and SQLite
    with pytest.raises(ValueError):
        test_engine = sqlalchemy.create_engine('sqlite:///:memory:')
        utils.copy_table_data(source_engine, test_engine)


def test_temp_engine():
    source_engine = utils.make_engine('tapdb', host='localhost')

    # table list
    temp_engine = utils.temp_engine(source_engine, ['source'])

    result = pd.read_sql_table('source', temp_engine)
    assert result.columns.tolist() == ['source_id', 'source_name']
    assert 'test.source.1' in result['source_name'].tolist()
    assert 'test.source.2' in result['source_name'].tolist()
    result = pd.read_sql_table('position', temp_engine)
    assert result.empty

    # table regex
    temp_engine = utils.temp_engine(source_engine, data_for_regex='sou*')

    result = pd.read_sql_table('source', temp_engine)
    assert result.columns.tolist() == ['source_id', 'source_name']
    assert 'test.source.1' in result['source_name'].tolist()
    assert 'test.source.2' in result['source_name'].tolist()
    result = pd.read_sql_table('position', temp_engine)
    assert result.empty


def test_in_memory_db():
    source_engine = tapdb.tapdb_engine(host='temp')
    memory_db = utils.in_memory_schema(source_engine, include_tables=['source'], include_regex='position*')

    # check that the tables are what they should be
    actual_tables = inspect(memory_db).get_table_names()
    expected_tables = ['source', 'position', 'positions_df']
    assert all(x in actual_tables for x in expected_tables)
    assert all(x in expected_tables for x in actual_tables)

    # check the data, use one table as test
    expected = utils.get_table(source_engine, 'data_source').sort_values('data_source_id')
    expected.index = range(len(expected))
    actual = utils.get_table(memory_db, 'data_source').sort_values('data_source_id')
    assert_frame_equal(actual, expected)

    # empty db
    memory_db = utils.in_memory_schema(source_engine, include_tables=None, include_regex=None)
    assert inspect(memory_db).get_table_names() == []


def test_id_from_name():
    tempdb = create_temp_tapdb()
    tapdb.insert_source(tempdb, 'test1')
    tapdb.insert_source(tempdb, 'test2')
    tapdb.insert_source(tempdb, 'test3')

    actual = utils.id_from_name(tempdb, 'source', 'test2')
    assert actual == 2


def test_ids_from_names():
    tempdb = create_temp_tapdb()
    tapdb.insert_source(tempdb, 'test1')
    tapdb.insert_source(tempdb, 'test2')
    tapdb.insert_source(tempdb, 'test3')

    # returning dict
    expected = {'test1': 1}
    actual = utils.ids_from_names(tempdb, 'source', ['test1'])
    assert actual == expected

    expected = {'test1': 1, 'test2': 2, 'test3': 3}
    actual = utils.ids_from_names(tempdb, 'source', ['test1', 'test2', 'test3'])
    assert actual == expected

    expected = {'test1': 1, 'test3': 3}
    actual = utils.ids_from_names(tempdb, 'source', ['test1', 'test3'])
    assert actual == expected

    expected = {'test1': 1, 'test3': 3}
    actual = utils.ids_from_names(tempdb, 'source', ['test1', 'GARBAGE', 'test3'])
    assert actual == expected

    expected = {}
    actual = utils.ids_from_names(tempdb, 'source', ['BAD', 'GARBAGE'])
    assert actual == expected

    # returning list
    expected = [1]
    actual = utils.ids_from_names(tempdb, 'source', ['test1'], return_dict=False)
    assert actual == expected

    expected = [1, 2, 3]
    actual = utils.ids_from_names(tempdb, 'source', ['test1', 'test2', 'test3'], return_dict=False)
    assert actual == expected

    expected = [1, 3]
    actual = utils.ids_from_names(tempdb, 'source', ['test1', 'test3'], return_dict=False)
    assert actual == expected

    expected = [1, 3, 2, 3, 1]
    actual = utils.ids_from_names(tempdb, 'source', ['test1', 'test3', 'test2', 'test3', 'test1'],
                                  return_dict=False)
    assert actual == expected

    with pytest.raises(ValueError):
        utils.ids_from_names(tempdb, 'source', ['test1', 'GARBAGE', 'test3'], return_dict=False)


def test_names_from_ids():
    tempdb = create_temp_tapdb()
    tapdb.insert_source(tempdb, 'test1')
    tapdb.insert_source(tempdb, 'test2')
    tapdb.insert_source(tempdb, 'test3')

    x = utils.id_from_name(tempdb, 'source', 'test2')
    actual = utils.names_from_ids(tempdb, 'source', [x])
    expected = ['test2']
    assert actual == expected

    y = utils.id_from_name(tempdb, 'source', 'test3', schema='main')
    actual = utils.names_from_ids(tempdb, 'source', [x, y])
    expected = ['test2', 'test3']
    assert actual == expected

    # repeated IDs
    actual = utils.names_from_ids(tempdb, 'source', [x, y, y, y], unique=True)
    expected = ['test2', 'test3']
    assert actual == expected

    actual = utils.names_from_ids(tempdb, 'source', [x, y, x, y])
    expected = ['test2', 'test3', 'test2', 'test3']
    assert actual == expected

    with pytest.raises(ValueError):
        utils.names_from_ids(tempdb, 'source', 1)


def test_name_exists():
    tempdb = create_temp_tapdb()
    tapdb.insert_source(tempdb, 'test1')
    assert utils.name_exists(tempdb, 'source', 'test1') is True
    assert utils.name_exists(tempdb, 'source', 'BAD') is False


def test_foreign_key_table():
    tempdb = sqlalchemy.create_engine('sqlite:///:memory:')
    meta = sqlalchemy.MetaData()
    _ = Table('parent_table_A', meta, Column('col_A', Integer, primary_key=True))
    _ = Table('parent_table_B', meta, Column('col_B', String, primary_key=True))
    _ = Table('child_table', meta,
              Column('col_with_fk_1', Integer, ForeignKey('parent_table_A.col_A'), primary_key=True,
                     nullable=False),
              Column('col_with_fk_2', String, ForeignKey('parent_table_B.col_B'), primary_key=True,
                     nullable=False),
              Column('col_no_key', String, primary_key=True, nullable=False))
    meta.reflect(bind=tempdb)
    meta.create_all(tempdb)

    actual = utils.foreign_key_table(tempdb, 'child_table', 'col_with_fk_1')
    assert actual == 'parent_table_A'

    actual = utils.foreign_key_table(tempdb, 'child_table', 'col_with_fk_2')
    assert actual == 'parent_table_B'

    with pytest.raises(ValueError):
        utils.foreign_key_table(tempdb, 'child_table', 'col_no_key')


def test_foreign_key_column():
    tempdb = sqlalchemy.create_engine('sqlite:///:memory:')
    meta = sqlalchemy.MetaData()
    _ = Table('parent_table_A', meta, Column('col_A', Integer, primary_key=True))
    _ = Table('parent_table_B', meta, Column('col_B', String, primary_key=True))
    _ = Table('child_table', meta,
              Column('col_with_fk_1', Integer, ForeignKey('parent_table_A.col_A'), primary_key=True,
                     nullable=False),
              Column('col_with_fk_2', String, ForeignKey('parent_table_B.col_B'), primary_key=True,
                     nullable=False),
              Column('col_no_key', String, primary_key=True, nullable=False))
    meta.reflect(bind=tempdb)
    meta.create_all(tempdb)

    actual = utils.foreign_key_column(tempdb, 'child_table', 'col_with_fk_1')
    assert actual == 'col_A'

    actual = utils.foreign_key_column(tempdb, 'child_table', 'col_with_fk_2')
    assert actual == 'col_B'

    with pytest.raises(ValueError):
        utils.foreign_key_column(tempdb, 'child_table', 'col_no_key')


def test_json():
    # use the test schema
    engine = utils.add_schema('temp_test', host='temp')

    # test with integer ID
    utils.add_persist_table(engine, 'json_int', mysql_engine='InnoDB', drop_first=False)

    # truncate sub-seconds from time as MySQL table only setup to second precision
    now = pd.Timestamp.now(tz='America/New_York').replace(microsecond=0)
    input_data = json.dumps({'a': 1, 'b': [5, 6, 'd'], 'c': {'c1': 'hello', 'c2': [7, 8]}})

    utils.insert_json(engine, 'json_int', 123, now, input_data)
    output_data = utils.get_json(engine, 'json_int', 123, now)

    assert input_data == output_data

    # test with VARCHAR ID, This will drop table first
    utils.add_persist_table(engine, 'json_char', int_id_type=False, drop_first=True)

    # truncate sub-seconds from time as MySQL table only setup to second precision
    now = pd.Timestamp.now(tz='America/New_York').replace(microsecond=0)
    input_data = json.dumps({'a': 1, 'b': [5, 6, 'd'], 'c': {'c1': 'hello', 'c2': [7, 8]}})

    utils.insert_json(engine, 'json_char', 'unit_test', now, input_data)
    output_data = utils.get_json(engine, 'json_char', 'unit_test', now)

    assert input_data == output_data
