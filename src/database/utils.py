"""
General helper functions related to databases.
"""

import functools
import os
import platform
import re
import socket
import tempfile
from pathlib import Path

import more_itertools
import pandas as pd
import sqlalchemy
from sqlalchemy import event, MetaData
from sqlalchemy.engine import Engine
import sqlalchemy_utils


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.execute("PRAGMA cache_size = 1000000000")
    cursor.close()


def make_engine(schema, host: str = 'localhost') -> Engine:
    """
    Return a sqlalchemy engine object for a database and schema.

    :param schema: name of the database schema
    :param host: host
    :return: sqlalchemy engine object
    """
    if host == "memory":
        return sqlalchemy.create_engine('sqlite:///:memory:')

    return sqlalchemy.create_engine(f'sqlite:///{database_filename(schema, host)}')


def attach_schema(engine: Engine, schema: str, host: str = 'localhost') -> Engine:
    """
    Attaches a database (schema) to an existing engine

    :param engine: sqlalchemy engine object
    :param schema: name of the database schema
    :param host: host
    :return: sqlalchemy engine object
    """

    filename = database_filename(schema, host)
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(f"ATTACH DATABASE '{filename}' AS '{schema}'"))
    return engine


def create_db(host: str, db_name: str, tables_metadata: MetaData = None):
    """
    Creates a new database if the existing database and schema does not exit.

    :param host: host
    :param db_name: database name
    :param tables_metadata: MetaData object that defines the tables to be created
    """
    if host == 'memory':
        engine = make_engine(db_name, host='memory')
    else:
        engine = make_engine(db_name, host=host)
        Path(engine.url.database).parent.mkdir(parents=True, exist_ok=True)

    if not sqlalchemy_utils.database_exists(engine.url):
        sqlalchemy_utils.create_database(engine.url)
        tables_metadata.create_all(engine)
        engine.dispose()


def copy_table_schema(from_engine, to_engine, exclude_tables=None, exclude_regex=None):
    """
    Copies the database schema from a source engine to a destination engine. This will overwrite all existing tables
    in the destination. The general use for this is to create mock databases using sqlite or temp schema in MySQL for
    unit test. This allows the mock database to have the same table setup as a production database.

    :param from_engine: sqlalchemy engine with the database schema to be copied from
    :param to_engine: sqlalchemy engine with the database schema to the pasted into
    :param exclude_tables: list of table names to exclude from the schema setup
    :param exclude_regex: regex of table names to exclude from the schema setup
    :return: nothing
    """
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=from_engine)

    # check if the database exists, if so drop, then create. Skip if SQLite memory engine
    if to_engine.url.database != ':memory:':
        if sqlalchemy_utils.database_exists(to_engine.url):
            sqlalchemy_utils.drop_database(to_engine.url)
        sqlalchemy_utils.create_database(to_engine.url, encoding='utf8mb4')

    # setup exclude tables by first eliminating ones that are in another schema, then the passed arguments
    remove_tables = [x for x in reversed(metadata.sorted_tables) if x.schema]
    if exclude_tables:
        remove_tables.extend([metadata.tables[x] for x in exclude_tables])
    if exclude_regex:
        regex = re.compile(exclude_regex)
        table_names = [x.name for x in metadata.sorted_tables]
        remove_tables.extend([metadata.tables[x] for x in table_names if regex.match(x)])
    for table in more_itertools.unique_everseen(remove_tables):  # to remove any duplicates
        metadata.remove(table)

    # create tables in new schema
    metadata.create_all(bind=to_engine)


def copy_table_data(from_engine, to_engine, include_tables=None, include_regex=None):
    """
    Copies the data from the from_engine to the to_engine. All data in all tables in the to_engine schema is deleted
    and then the data in the include_tables and include_regex are copied from the from_engine to the to_engine. To
    copy all data from all tables use include_regex='.*'

    Currently this only works if the form and to engine are on the same database server (ie: both MySQL)

    :param from_engine: sqlalchemy engine with the database schema to be copied from
    :param to_engine: sqlalchemy engine with the database schema to the pasted into
    :param include_tables: list of table names to copy
    :param include_regex: regex of table names to match for copy. Use ".*" for all
    :return: nothing
    """
    if from_engine.url.get_backend_name() != to_engine.url.get_backend_name():
        raise ValueError('to_engine and from_engine must both be on the same database server.')

    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=to_engine)
    from_schema = from_engine.url.database

    # remove all tables that are in another schema
    exclude_tables = [x for x in reversed(metadata.sorted_tables) if x.schema]
    for exclude_table in exclude_tables:
        metadata.remove(exclude_table)

    # remove all data from all tables
    with to_engine.begin() as conn:
        for table in reversed(metadata.sorted_tables):
            conn.execute(table.delete())

    # create list of tables to copy data
    copy_tables = []
    if include_tables:
        copy_tables.extend(include_tables)
    if include_regex:
        regex = re.compile(include_regex)
        table_names = [x.name for x in metadata.sorted_tables]
        copy_tables.extend([x for x in table_names if regex.match(x)])

    # copy the data from_engine to to_engine
    with to_engine.begin() as conn:
        for copy_table in copy_tables:
            sql = sqlalchemy.text('INSERT INTO ' + copy_table + ' SELECT * FROM ' + from_schema + '.' + copy_table)
            conn.execute(sql)


def temp_engine(from_engine, data_for_tables=None, data_for_regex=None):
    """
    Create a temporary schema and associated tables in MySQL. For a selection of the tables copy the data as well.

    :param from_engine: sqlalchemy engine to the source
    :param data_for_tables: list of tables to copy the data
    :param data_for_regex: regex of tables to copy the data
    :return: sqlalchemy engine for the temporary schema
    """
    schema = from_engine.url.database
    temp_schema = 'temp_' + schema
    to_engine = make_engine(temp_schema, from_engine.url.host)

    copy_table_schema(from_engine, to_engine)
    copy_table_data(from_engine, to_engine, data_for_tables, data_for_regex)

    return to_engine


def in_memory_schema(source_engine, include_tables=None, include_regex=None):
    """
    Creates an in-memory copy of a database schema, table and contents. Can be used as a cached version of the on-disk
    database. The engine is sqlalchemy sqlite, as such it can only be a copy of one database or "schema" in MySQL and
    none of tables copied can have foreign key references to a table in another schema.

    The selection of the tables to include in the copy can be either or both of the named list (include_tables) or
    a regex pattern (include_regex) to match table names.

    :param source_engine: sqlalchemy engine object of the source database
    :param include_tables: list of table names to be copied, or None to use include_regex only.
    :param include_regex: regex expression to match tables to be copied, or None to use include_tables only
    :return: sqlite sqlalchemy engine with tables and data copied from source engine
    """
    memory_engine = sqlalchemy.create_engine('sqlite:///:memory:')

    # Get the meta data of the source database
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=source_engine)
    source_table_names = [x.name for x in metadata.sorted_tables]

    # filter the list of tables to create a list, good_list, of tables to be included
    good_tables = []
    if include_tables:
        good_tables += include_tables
    if include_regex:
        regex = re.compile(include_regex)
        good_tables += [x for x in source_table_names if regex.match(x)]

    # exclude all tables that are not in the good_tables list and then create the good_tables in the memory db
    exclude_tables = [x for x in metadata.tables.keys() if x not in good_tables]
    for exclude_table in exclude_tables:
        metadata.remove(metadata.tables[exclude_table])
    metadata.create_all(bind=memory_engine)

    # copy all the data from the source db to the in memory db
    for table in good_tables:
        data = pd.read_sql_table(table, source_engine)
        # chunksize required as of pandas 0.23 or will exceed sqlite3 limits. 100 is arbitrary if slow look to test
        # what the number should be or make it dynamically calculated
        data.to_sql(table, memory_engine, if_exists='append', index=False, chunksize=100)
    return memory_engine


def database_names(username, password, host='localhost'):
    """
    Get the names of all database schema on a DB host

    :param username: username to connect
    :param password: password to connect
    :param host: host name
    :return: list of database names
    """
    engine = sqlalchemy.create_engine(f'mysql+mysqldb://{username}:{password}@{host}')
    with engine.begin() as conn:
        result = conn.execute(sqlalchemy.text("SHOW DATABASES"))
    return [x[0] for x in result.fetchall()]  # get the first element of the tuple returned from sql


def base_data_directory(host='localhost'):
    """
    Returns the Path directory for the base data directory. If localhost is used then the data directory on the local
    machine is returned. If temp is used then a directory in the temp directory will be created and returned.

    :param host: machine host name, or localhost or temp
    :return: Path object
    """
    if host == 'temp':
        return Path(tempfile.gettempdir()) / "puma"

    # if the host is the currently running machine, change to localhost
    if host.lower() == socket.gethostname().lower():
        host = 'localhost'

    if host == 'localhost':
        if platform.system() == "Windows":
            return Path(os.environ.get('LOCALAPPDATA')) / "puma"
        else:
            return Path('/var/lib/nova')
    else:
        if platform.system() == "Windows":
            return Path('//' + host + '/puma')
        else:
            return Path('/mnt/' + host + '/puma')


def database_filename(schema, host='localhost'):
    """
    Returns the Path filename for a database. If localhost is used then the data directory on the local
    machine is returned. If temp is used then a directory in the temp directory will be created and returned.

    :param schema: database schema name
    :param host: machine host name, or localhost or temp
    :return: Path object
    """
    return Path(str(base_data_directory(host) / "database" / schema) + ".db")


def get_table(engine, table_name):
    """

    :param engine: sqlalchemy engine
    :param table_name: name of the table
    :return: DataFrame of the table contents
    """
    return pd.read_sql_table(table_name, engine)


def name_exists(engine, table_name, name, schema=None):
    """
    Test if a name exists in a standard name/id table that has column layout of {X}_id, {X}_name for table name {X}.

    :param engine: sqlalchemy engine
    :param table_name: name of the table {X}
    :param name: name of the {X}_name
    :param schema: if provided then will use this schema to prepend to the table name
    :return: True if the name exists, False if not
    """
    column_name = table_name + "_name"
    schema_table = schema + '.' + table_name if schema else table_name
    sql = sqlalchemy.text('SELECT COUNT(*) FROM ' + schema_table + ' WHERE ' + column_name + ' = "' + name + '"')
    with engine.begin() as conn:
        result = conn.execute(sql).fetchall()
    return result[0][0] > 0  # return True if there is one, else False


def id_from_name(engine, table_name, name, schema=None):
    """
    For tables that have the standard column layout of {X}_id, {X}_name for a table name {X}. If the schema is provided
    then it can lookup a value from a table in a schema different from the engine. Example is a table named "data" and
    the columns "data_id" and "data_name"

    :param engine: sqlalchemy engine
    :param table_name: name of the table
    :param name: name of the {X}_name
    :param schema: if provided then will use this schema to prepend to the table name
    :return: the {X}_id
    """

    column_name = table_name + "_name"
    column_id = table_name + "_id"
    schema_table = schema + '.' + table_name if schema else table_name
    sql = sqlalchemy.text(
        'SELECT ' + column_id + ' FROM ' + schema_table + ' WHERE ' + column_name + ' = "' + name + '"')
    with engine.begin() as conn:
        result = conn.execute(sql).fetchall()
    if len(result) == 0:
        raise ValueError("The name value: " + name + ", does not exist in table: " + table_name + ".")
    else:
        return result[0][0]


def ids_from_names(engine, table_name, name_list, schema=None, return_dict=True):
    """
    For tables that have the standard column layout of {X}_id, {X}_name for a table name {X}. Example is a table
    named "data" and the columns "data_id" and "data_name"

    :param engine: sqlalchemy engine
    :param table_name: name of the table
    :param name_list: list of names
    :param schema: optional database schema
    :param return_dict: if True then the return is a dictionary, if False a list of ids
    :return: dictionary with keys = names and values = ids or list of ids
    """

    column_name = table_name + "_name"
    column_id = table_name + "_id"

    if return_dict:
        table_name = schema + '.' + table_name if schema else table_name
        sql = 'SELECT ' + column_name + ',' + column_id + ' FROM ' + table_name + \
              ' WHERE ' + column_name + ' in ' '(' + '"' + '","'.join(name_list) + '"' + ')'
        with engine.begin() as conn:
            result = conn.execute(sqlalchemy.text(sql)).fetchall()
        keys = [x[0] for x in result]
        values = [x[1] for x in result]
        return dict(zip(keys, values))
    else:
        table_df = pd.read_sql_table(table_name, engine, schema)
        sr = pd.DataFrame({column_name: name_list}).merge(table_df, how='left', on=column_name)[column_id]
        if any(sr.isnull()):
            raise ValueError('bad ID that could not match to a name')
        return sr.tolist()


def names_from_ids(engine, table_name, id_list, unique=False, schema=None):
    """
    For tables that have the standard column layout of {X}_id, {X}_name for a table name {X}. Example is a table
    named "data" and the columns "data_id" and "data_name". If the unique flag is False, then the return list will
    be in the same order as the id_list and will contain duplicates if there are duplicates in the id_list. If unique
    is True then the return list may be in any order and only contains unique values.

    :param engine: sqlalchemy engine
    :param table_name: name of table {X}
    :param id_list: a list of id values
    :param unique: if False then each id in the list will correspond to a name in the returned list. If True then the
                   return list is unique list of the names without repeats
    :param schema: database schema, or None
    :return: list of names
    """

    if not isinstance(id_list, list):
        raise ValueError("id_list must be a list. If you want to pass a single value use [x].")

    column_name = table_name + "_name"
    column_id = table_name + "_id"

    table_df = pd.read_sql_table(table_name, engine, schema)

    if unique:
        return table_df[table_df[column_id].isin(id_list)][column_name].tolist()
    else:
        return pd.DataFrame({column_id: id_list}).merge(table_df, how='left', on=column_id)[column_name].tolist()


def upload_name(engine, table_name, value, update=None):
    """
    For tables that have the standard column layout of {X}_id, {X}_name for a table name {X}. Example is a table
    named "data" and the columns "data_id" and "data_name"

    :param engine: sqlalchemy engine
    :param table_name: name of the table
    :param value: {X}_name to upload
    :param update: if True then update, otherwise if False perform insert
    :return: result of the sqlalchemy insert or update function
    """
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name, meta, autoload_with=engine.engine)

    column_name = table_name + "_name"
    if update is None:
        command = table.insert().values(**{column_name: value})
    else:
        command = table.update(). \
            where(getattr(table.c, column_name) == value). \
            values(**{column_name: update})
    with engine.begin() as conn:
        result = conn.execute(command)
    return result


@functools.lru_cache(maxsize=32)
def foreign_key_table(engine, table_name, column_name):
    """
    For a given column_name in a table_name, returns the table that contains the foreign key reference. For example
    if there is a table_child with column_child that has a foreign key reference to column_parent in table_parent
    this will return the name "table_parent"

    This is only tested to work with a column that has a single foreign key reference. Unknown behavior if there
    are multiple foreign keys.

    :param engine: sqlalchemy engine
    :param table_name: name of the table that has the foreign key reference
    :param column_name: name of the column that has the foreign key reference
    :return: name of the table that contains the foreign key
    """
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name, meta, autoload_with=engine.engine)
    fkc_set = table.foreign_key_constraints

    foreign_table_name = None
    for fkc in fkc_set:
        if fkc.column_keys[0] == column_name:
            foreign_table_name = fkc.referred_table.name
            break
    if foreign_table_name is None:
        raise ValueError("No foreign table found for table_name and column_name")
    return foreign_table_name


def foreign_key_column(engine, table_name, column_name):
    """
    For a given column_name in a table_name, returns the column that contains the foreign key reference. For example
    if there is a table_child with column_child that has a foreign key reference to column_parent in table_parent
    this will return the name "column_parent"

    This is only tested to work with a column that has a single foreign key reference. Unknown behavior if there
    are multiple foreign keys.

    :param engine: sqlalchemy engine
    :param table_name: name of the table that has the foreign key reference
    :param column_name: name of the column that has the foreign key reference
    :return: name of the column that is the foreign key
    """
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name, meta, autoload_with=engine.engine)
    fkc_set = table.foreign_key_constraints

    foreign_col_name = None
    for fkc in fkc_set:
        if fkc.column_keys[0] == column_name:
            foreign_col_name = fkc.elements[0].get_referent(fkc.referred_table).name
            break
    if foreign_col_name is None:
        raise ValueError("No foreign column found for table_name and column_name")
    return foreign_col_name


def data_table_from_ts_name(engine, time_series_name):
    """
    Given a time_series_name will return what data table the data is in.

    :param engine: sqlalchemy engine
    :param time_series_name: name of the time series
    :return: name of the data table that the time series uses to store its data
    """
    # noinspection SyntaxError
    sql = 'SELECT data_table_name FROM data_table WHERE data_table_id = ' \
          '(SELECT data_table_id FROM time_series WHERE time_series_name = "' + time_series_name + '")'
    with engine.begin() as conn:
        result = conn.execute(sqlalchemy.text(sql)).fetchone()[0]
    return result


def engine_database(engine):
    if engine.url.database == 'tsdb':
        return 'tsdb'
    elif engine.url.database == ':memory:':
        return 'sqlite'
    else:
        return 'symboldb'


def add_schema(schema: str, username: str, password: str, db_host: str) -> sqlalchemy.engine.Engine:
    """
    Add a new schema to a database if it does not exist, if it does exist then do nothing.

    :param schema: schema name
    :param username: database username
    :param password: database password
    :param db_host: database host
    :return: sqlalchemy engine for the schema
    """
    eng = make_engine(schema, db_host)
    # if it exists, ignore and keep, otherwise create
    if not sqlalchemy_utils.database_exists(eng.url):
        sqlalchemy_utils.create_database(eng.url)
    return eng


def add_persist_table(engine, table_name: str, int_id_type=True, mysql_engine='InnoDB', drop_first=False):
    """
    Add a standard persistence table to the database. If drop_first is True then the table will be deleted and
    recreated if it already exists. If not then the new table will only be added if it does not already exist.

    :param engine: sqlalchemy engine
    :param table_name: table name
    :param int_id_type: if True then the ID column will be INTEGER, otherwise VARCHAR if False
    :param mysql_engine: mysql engine type
    :param drop_first: if True then the table will be dropped first if it exists
    :return: nothing
    """
    metadata = sqlalchemy.MetaData()
    sa_id_type = sqlalchemy.Integer if int_id_type else sqlalchemy.String(200)
    table = sqlalchemy.Table(table_name, metadata,
                             sqlalchemy.Column('id', sa_id_type, primary_key=True, nullable=False),
                             sqlalchemy.Column('datetime', sqlalchemy.DateTime, primary_key=True, nullable=False),
                             sqlalchemy.Column('json', sqlalchemy.JSON), mysql_engine=mysql_engine,
                             must_exist=False)
    if drop_first:
        table.drop(engine, checkfirst=True)
    table.create(engine, checkfirst=True)


def insert_json(engine, table: str, id, datetime, data: str) -> str:
    """
    Inserts a json into a standard json table.

    :param engine: sqlalchemy engine
    :param table: table name
    :param id: ID
    :param datetime: pandas timestamp of the datetime
    :param data: string that is the json data
    :return: result of sqlalchemy insert statement
    """
    datetime = datetime.tz_convert('UTC').tz_localize(None)
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table, meta, autoload_with=engine.engine)
    command = table.insert().values(id=id, datetime=datetime, json=data)
    with engine.begin() as conn:
        result = conn.execute(command)
    return result


def get_json(engine, table: str, id, datetime) -> str:
    """
    Get a json from a standard json table.

    :param engine: sqlalchemy engine
    :param table: table name
    :param id: ID
    :param datetime: pandas timestamp of the datetime
    :return: string that is the json data
    """
    datetime = datetime.tz_convert('UTC').tz_localize(None)
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table, meta, autoload_with=engine.engine)
    request = sqlalchemy.select(table.c.json).where(sqlalchemy.and_(table.c.id == id, table.c.datetime == datetime))
    with engine.begin() as conn:
        result = conn.execute(request)
    return result.fetchone()[0]
