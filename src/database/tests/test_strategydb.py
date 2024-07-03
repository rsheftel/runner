"""
unit tests for strategydb
"""
import pandas as pd
import pytest
import sqlalchemy
import sqlalchemy_utils
from pandas.testing import assert_frame_equal
from sqlalchemy import Engine

import database.utils as utils
import utils.file as futils
from database import strategydb

# Global variables
strategydb_test = Engine(None, None, None)


def setup_module():
    global strategydb_test
    strategydb.delete_db("temp")
    strategydb.create_db("temp")
    strategydb_test = strategydb.engine(host='temp')


def teardown_module():
    strategydb_test.dispose()


def create_temp_strategydb():
    tempdb = sqlalchemy.create_engine('sqlite:///:memory:')
    utils.copy_table_schema(strategydb_test, tempdb)
    return tempdb


def test_engine():
    assert isinstance(strategydb_test, sqlalchemy.engine.Engine)


def test_create_db():
    directory = utils.base_data_directory('temp')
    futils.delete_dir(directory / "database")

    strategydb.create_db('temp')
    engine = strategydb.engine('temp')
    assert sqlalchemy_utils.database_exists(engine.url)


def test_strategy_upsert():
    engine = create_temp_strategydb()

    strats = strategydb.get_strategies(engine)
    assert strats.empty

    # insert a strategy
    strategydb.insert_strategy(engine, 'strat_01', 'mod_1', 'class_1')
    strategydb.insert_strategy(engine, 'strat_02')

    expected = pd.DataFrame({'strategy_name': ['strat_01', 'strat_02'], 'module_name': ['mod_1', None],
                             'class_name': ['class_1', None]}, columns=['strategy_name', 'module_name', 'class_name'])
    actual = strategydb.get_strategies(engine)
    assert_frame_equal(actual, expected)

    # update strategy_name
    # fail because not a string
    with pytest.raises(ValueError):
        strategydb.update_strategy(engine, 'strat_02', 33)

    # valid
    strategydb.update_strategy(engine, 'strat_02', 'strat_33')

    expected.iloc[1, 0] = 'strat_33'
    actual = strategydb.get_strategies(engine)
    assert_frame_equal(actual, expected)

    # update details
    strategydb.update_strategy_details(engine, 'strat_01', class_name='classy')
    strategydb.update_strategy_details(engine, 'strat_33', 'new_mod', 'new_class')

    expected = pd.DataFrame({'strategy_name': ['strat_01', 'strat_33'], 'module_name': ['mod_1', 'new_mod'],
                             'class_name': ['classy', 'new_class']},
                            columns=['strategy_name', 'module_name', 'class_name'])
    actual = strategydb.get_strategies(engine)
    assert_frame_equal(actual, expected)
