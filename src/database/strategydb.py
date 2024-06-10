"""
All functions for uploading and accessing strategy data in StrategyDB
"""
import pandas as pd
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, Integer, String, Engine

import database.utils as dbutils


def create_db(host: str):
    metadata_obj = MetaData()
    strategy = Table(
        "strategy",
        metadata_obj,
        Column("strategy_id", Integer, primary_key=True, autoincrement=True, unique=True),
        Column("strategy_name", String(45), nullable=False, unique=True),
        Column("module_name", String(128), nullable=False),
        Column("class_name", String(128), nullable=False),
    )
    dbutils.create_db(host, 'strategy', metadata_obj)


def strategydb_engine(host: str = 'linuxdb') -> Engine:
    """
    Get the sqlalchemy engine object for the StategyDB to be used in all the strategydb functions

    :param host: host machine that has the database
    :return: sqlalchemy engine
    """
    return dbutils.make_engine('strategy', host=host)


def get_strategies(engine):
    """
    Get a DataFrame of the strategy names and other columns, with the ID excluded

    :param engine: sqlalchemy engine for the schema
    :return: DataFrame
    """
    return pd.read_sql_table('strategy', engine).drop('strategy_id', axis=1)


def insert_strategy(engine, strategy_name, module_name=None, class_name=None):
    """
    Inserts a new strategy into the strategy table.

    :param engine: sqlalchemy engine
    :param strategy_name: strategy name
    :param module_name: module name of the class
    :param class_name: class name
    :return: result of sqlalchemy insert function
    """
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table('strategy', meta, autoload_with=engine.engine)
    command = table.insert().values(strategy_name=strategy_name, module_name=module_name, class_name=class_name)
    with engine.begin() as conn:
        result = conn.execute(command)
    return result


def update_strategy(engine, current_name, new_name):
    """
    Change a strategy's name

    :param engine: sqlalchemy engine for the schema
    :param current_name: the current name of the strategy to change from
    :param new_name: the new name of the strategy to change to
    :return: result of the sqlalchemy call to update
    """
    if (not isinstance(current_name, str)) or (not isinstance(new_name, str)):
        raise ValueError("strategy_name must be a string.")
    return utils.upload_name(engine, 'strategy', current_name, update=new_name)


def update_strategy_details(engine, strategy_name, module_name=None, class_name=None):
    """
    Updates row in the strategy table. If any parameter is None that column will be ignored in the update.

    :param engine: sqlalchemy engine for the schema
    :param strategy_name: strategy name
    :param module_name: new module name, or None to ignore for update
    :param class_name: new class name, or None to ignore for update
    :return: result of the sqlalchemy call to update
    """
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table('strategy', meta, autoload_with=engine.engine)
    arguments = {'module_name': module_name, 'class_name': class_name}
    update_dict = {x: arguments[x] for x in arguments if arguments[x]}
    command = table.update().where(table.c.strategy_name == strategy_name).values(update_dict)
    with engine.begin() as conn:
        result = conn.execute(command)
    return result
