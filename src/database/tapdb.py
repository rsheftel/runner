"""
All functions for uploading and accessing strategy data in TAPDB Trades and Positions DB
"""

from database import metadb
import database.utils as utils
import pandas as pd
import raccoon as rc
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, Integer, DateTime, JSON, Numeric, ForeignKey, String


def create_db(host: str):
    metadata_obj = MetaData()
    Table(
        "orders_df",
        metadata_obj,
        Column("id", Integer, primary_key=True, nullable=False),
        Column("datetime", DateTime, primary_key=True, nullable=False),
        Column("json", JSON),
    )
    Table(
        "position",
        metadata_obj,
        Column("source_id", Integer, ForeignKey("source.source_id"), primary_key=True, nullable=False),
        Column("strategy_id", Integer, primary_key=True, nullable=False),  # ForeignKey("strategy.strategy_id")
        Column("product_type_id", Integer, primary_key=True, nullable=False),
        Column("symbol_id", Integer, primary_key=True, nullable=False),
        Column("datetime", DateTime, primary_key=True, nullable=False),
        Column("position", Numeric(24, 10)),
    )
    Table(
        "positions_df",
        metadata_obj,
        Column("id", Integer, primary_key=True, nullable=False),
        Column("datetime", DateTime, primary_key=True, nullable=False),
        Column("json", JSON),
    )
    Table(
        "source",
        metadata_obj,
        Column("source_id", Integer, primary_key=True, nullable=False, unique=True, autoincrement=True),
        Column("source_name", String, nullable=False, unique=True),
    )

    utils.create_db(host, 'tapdb', metadata_obj)


def tapdb_engine(host: str = 'linuxdb'):
    """
    Get the sqlalchemy engine object for the StategyDB to be used in all the tapdb functions

    :param host: host machine that has the database
    :return: sqlalchemy engine
    """
    engine = utils.make_engine('tapdb', host)
    engine = utils.attach_schema(engine, 'strategy', host)
    return engine


def delete_db(host: str) -> None:
    """
    Deletes the TAPDB on the host

    :param host: host machine that has the database
    """
    utils.delete_db(host, "tapdb")


def get_sources(engine):
    """
    Get a DataFrame of the source names and other columns, with the ID excluded

    :param engine: sqlalchemy engine for the schema
    :return: DataFrame
    """
    return pd.read_sql_table('source', engine).drop('source_id', axis=1)


def insert_source(engine, source_name):
    """
    Inserts a new source into the source table.

    :param engine: sqlalchemy engine
    :param source_name: source name
    :return: result of sqlalchemy insert function
    """
    return utils.upload_name(engine, 'source', source_name)


def update_source(engine, current_name, new_name):
    """
    Change a source name

    :param engine: sqlalchemy engine for the schema
    :param current_name: the current name of the source to change from
    :param new_name: the new name of the source to change to
    :return: result of the sqlalchemy call to update
    """
    if (not isinstance(current_name, str)) or (not isinstance(new_name, str)):
        raise ValueError("source_name must be a string.")
    return utils.upload_name(engine, 'source', current_name, update=new_name)


def max_datetime(engine, source=None, strategy=None):
    """
    Get the maximum datetime from the positions table. Can be optionally filtered by source or strategy.

    :param engine: sqlalchemy engine
    :param source: if provided will filter by the source, otherwise None will be ignored
    :param strategy: if provided will filter by the strategy, otherwise None will be ignored
    :return: pandas Timestamp of the maximum datetime
    """

    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table('position', meta, autoload_with=engine.engine)

    request = sqlalchemy.select(sqlalchemy.func.max(table.c.datetime))
    if source:
        source_id = utils.id_from_name(engine, 'source', source)
        request = request.where(table.c.source_id == source_id)

    if strategy:
        strategy_id = utils.id_from_name(engine, 'strategy', strategy, 'strategy')
        request = request.where(table.c.strategy_id == strategy_id)

    with engine.begin() as conn:
        result = conn.execute(request)
    date_str = result.fetchone()[0]
    return pd.Timestamp(date_str, tz='UTC') if date_str else None


def get_positions(engine, source=None, strategy=None, datetime=None):
    """
    Get the positions optionally filtered by source, strategy and/or datetime.

    :param engine: sqlalchemy engine
    :param source: if provided only this source will be returned
    :param strategy: if provided then only this strategy will be returned
    :param datetime: date time to filter for, otherwise if None all datetimes are returned
    :return: DataFrame of the results
    """

    sql = 'SELECT source_name AS source, strategy.strategy_name AS strategy, product_type_id, symbol_id, ' \
          'datetime, position ' \
          'FROM position ' \
          'INNER JOIN source ON position.source_id=source.source_id ' \
          'INNER JOIN strategy.strategy ON position.strategy_id=strategy.strategy.strategy_id'

    # optional filter columns
    multi_where = False
    if datetime:
        datetime_str = datetime.tz_convert('UTC').strftime('%Y-%m-%d %H:%M:%S.%f')
        sql += " WHERE datetime='" + datetime_str + "'"
        multi_where = True

    if source:
        sql += " AND" if multi_where else " WHERE"
        sql += " source_name='" + source + "'"
        multi_where = True

    if strategy:
        sql += " AND" if multi_where else " WHERE"
        sql += " strategy_name='" + strategy + "'"

    # read from the Database
    df = pd.read_sql_query(sqlalchemy.text(sql), engine, parse_dates=['datetime'])

    # assign time_zone of UTC to the DateTime column
    df['datetime'] = df['datetime'].apply(lambda x: x.tz_localize('UTC'))

    # apply the product_type and symbol names
    for ID in df['product_type_id'].unique():
        match_rows = df['product_type_id'] == ID
        product_type = metadb.product_types[ID]
        df.loc[match_rows, 'product_type'] = product_type
        df.loc[match_rows, 'symbol'] = \
            utils.names_from_ids(engine, 'symbol', df.loc[match_rows, 'symbol_id'].tolist(), schema=product_type)

    # empty results means no product_type and symbol columns were created above to create them as empty now
    if df.empty:
        df = df.join(pd.DataFrame(columns=['product_type', 'symbol']))

    return df[['source', 'strategy', 'product_type', 'symbol', 'datetime', 'position']]


def insert_position(engine, source, strategy, product_type, symbol, datetime, position):
    """
    Insert a single position into TAPDB

    :param engine: sqlalchemy engine
    :param source: source name
    :param strategy: strategy name
    :param product_type: product type name
    :param symbol: symbol name
    :param datetime: pandas Timestamp
    :param position: position amount
    :return: result of sqlalchemy execute statement
    """

    # get the ids from names
    source_id = utils.id_from_name(engine, 'source', source)
    strategy_id = utils.id_from_name(engine, 'strategy', strategy, 'strategy')
    product_type_id = metadb.product_types.index(product_type)
    symbol_id = utils.id_from_name(engine, 'symbol', symbol, product_type)
    # convert datetime to UTC
    datetime_utc = datetime.tz_convert('UTC').to_pydatetime()

    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table('position', meta, autoload_with=engine.engine)

    command = table.insert().values(source_id=str(source_id),
                                    strategy_id=str(strategy_id),
                                    product_type_id=str(product_type_id),
                                    symbol_id=str(symbol_id),
                                    datetime=datetime_utc,
                                    position=float(position)
                                    )

    with engine.begin() as conn:
        result = conn.execute(command)
    return result


def insert_positions(engine, positions_df):
    """
    Insert a Dataframe of positions into TAPDB. The DataFrame must have the columns of [source, strategy, product_type,
    symbol, datetime (as Pandas Timestamp with timezone), position]

    :param engine: sqlalchemy engine
    :param positions_df: pandas DataFrame
    :return: nothing
    """

    # if the dataframe is empty just return
    if positions_df.empty:
        return

    # so we don't alter the input dataframe
    positions_df = positions_df.copy()

    # apply the product_type and symbol IDs
    for product_type in positions_df['product_type'].unique():
        match_rows = positions_df['product_type'] == product_type
        product_type_id = metadb.product_types.index(product_type)
        positions_df.loc[match_rows, 'product_type_id'] = product_type_id

        positions_df.loc[match_rows, 'symbol_id'] = \
            utils.ids_from_names(engine, 'symbol', positions_df.loc[match_rows, 'symbol'].tolist(),
                                 schema=product_type, return_dict=False)

    # make sure everything is astype int
    positions_df['product_type_id'] = positions_df['product_type_id'].astype(int)
    positions_df['symbol_id'] = positions_df['symbol_id'].astype(int)

    # get the strategy ID and the source ID
    positions_df['strategy_id'] = utils.ids_from_names(engine, 'strategy', positions_df['strategy'].tolist(),
                                                       'strategy', return_dict=False)

    positions_df['source_id'] = utils.ids_from_names(engine, 'source', positions_df['source'].tolist(),
                                                     return_dict=False)

    # convert datetimes to UTC and strip time zone
    positions_df['datetime'] = [x.tz_convert('UTC').tz_localize(None) for x in positions_df['datetime']]

    # insert the dataframe
    upload_df = positions_df[['source_id', 'strategy_id', 'product_type_id', 'symbol_id', 'datetime', 'position']]
    upload_df.to_sql('position', engine, if_exists='append', index=False)


def insert_df(engine: sqlalchemy.engine.Engine, table: str, source: str, datetime, data_frame: rc):
    """
    Insert the DataFrame into TAPDB as a json object.

    :param engine: sqlalchemy engine
    :param table: table name
    :param source: source name
    :param datetime: datetime of the save
    :param data_frame: the DataFrame to insert
    :return: nothing
    """
    df_json = data_frame.to_json()
    source_id = utils.id_from_name(engine, 'source', source)
    utils.insert_json(engine, table, source_id, datetime, df_json)


def get_df(engine: sqlalchemy.engine.Engine, table: str, source: str, datetime) -> rc:
    """
    Get the DataFrame as a raccoon DataFrame from TAPDB

    :param engine: sqlalchemy engine
    :param table: table name
    :param source: source name
    :param datetime: datetime of the save to get
    :return: raccoon DataFrame
    """
    source_id = utils.id_from_name(engine, 'source', source)
    df_json = utils.get_json(engine, table, source_id, datetime)
    return rc.DataFrame.from_json(df_json)


def insert_positions_df(engine: sqlalchemy.engine.Engine, source: str, datetime, positions_df: rc):
    """
    Insert the positions_df DataFrame into TAPDB as a json object.

    :param engine: sqlalchemy engine
    :param source: source name
    :param datetime: datetime of the save
    :param positions_df: the positions_df from the PositionManager
    :return: nothing
    """
    insert_df(engine, 'positions_df', source, datetime, positions_df)


def get_positions_df(engine: sqlalchemy.engine.Engine, source: str, datetime) -> rc:
    """
    Get the positions_df as a raccoon DataFrame from TAPDB

    :param engine: sqlalchemy engine
    :param source: source name
    :param datetime: datetime of the save to get
    :return: raccoon DataFrame
    """
    return get_df(engine, 'positions_df', source, datetime)


def insert_orders_df(engine: sqlalchemy.engine.Engine, source: str, datetime, orders_df: rc):
    """
    Insert the orders_df DataFrame into TAPDB as a json object.

    :param engine: sqlalchemy engine
    :param source: source name
    :param datetime: datetime of the save
    :param orders_df: the orders_df from the OrderManager
    :return: nothing
    """
    insert_df(engine, 'orders_df', source, datetime, orders_df)


def get_orders_df(engine: sqlalchemy.engine.Engine, source: str, datetime) -> rc:
    """
    Get the orders_df as a raccoon DataFrame from TAPDB

    :param engine: sqlalchemy engine
    :param source: source name
    :param datetime: datetime of the save to get
    :return: raccoon DataFrame
    """
    return get_df(engine, 'orders_df', source, datetime)
