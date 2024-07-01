"""
Position Manager class
"""

import collections
import copy
import itertools
import logging
import operator
import uuid

import pandas as pd
import raccoon as rc

import utils.collections as cutils
import utils.pandas as pdutils
from database import metadb, tapdb

log = logging.getLogger(__name__)


class PositionManager:
    def __init__(self, position_manager_id, order_manager, tapdb_engine):
        """
        :param position_manager_id: unique string ID
        :param order_manager: OrderManager object
        :param tapdb_engine: sqlalchemy engine for TAPDB
        """
        self.__uuid = str(uuid.uuid4())
        self._position_manager_id = position_manager_id
        self._order_manager = order_manager
        self._tapdb = tapdb_engine
        self._market_data_manager = None
        self._metadb = None
        self._live_frequency = None
        self._new_trades = []
        self._initialize_positions_df()
        self._trade_id = None
        self._eod_metrics = collections.OrderedDict()
        log.info(f'PositionManager initialized: {self}')

    @property
    def uuid(self):
        return self.__uuid

    @property
    def id(self):
        return self._position_manager_id

    @property
    def order_manager(self):
        return self._order_manager

    @property
    def positions_df(self):
        return self._positions_df

    def _initialize_positions_df(self):
        """
        Initialize a new positions_df and delete all existing entries. Dangerous to call, do not call direct.

        :return: nothing
        """
        log.info('initializing a new empty positions_df')
        self._positions_df = rc.DataFrame(columns=['current_position', 'start_position', 'net_quantity', 'buy_quantity',
                                                   'sell_quantity', 'buy_avg_price', 'sell_avg_price', 'buy_pnl',
                                                   'sell_pnl', 'trade_pnl', 'position_pnl', 'gross_pnl', 'commission',
                                                   'net_pnl', 'prior_close_price', 'current_price'],
                                          index_name=('strategy_id', 'product_type', 'symbol'), sort=True)

    def _reset_positions_df(self):
        """
        Resets the positions_df and delete all existing entries but perserves the container lists so that other objects
        that reference the positions_df will keep the pointer references. Dangerous to call, do not call direct.

        :return: nothing
        """
        log.info('initializing a new empty positions_df')
        self._positions_df.delete_all_rows()

    def setup_market_data(self, market_data_manager, live_frequency='1min'):
        """
        Connect to the MarketDataManager for pnl calculations.

        :param market_data_manager: MarketDataManager object
        :param live_frequency: frequency to use for getting the live price for pnl calculation
        :return: nothing
        """
        log.info(f'setting up market data (mdm, live_frequency): ({market_data_manager}, {live_frequency})')
        self._market_data_manager = market_data_manager
        self._live_frequency = live_frequency
        db_info = market_data_manager.database_info()
        self._metadb = metadb.metadb_engine(db_info['username'], db_info['password'], db_info['host'])

    @property
    def new_trades(self):
        """
        New trades since initialization of PositionManager

        :return: Dictionary of all new trades
        """
        return self._new_trades

    @property
    def new_trades_df(self):
        """
        New trades since initialization of PositionManager

        :return: DataFrame of all new trades
        """
        if self._new_trades:
            return rc.DataFrame(cutils.invert_list_of_dict(self._new_trades))
        else:
            return None

    def get_value(self, strategy_id, product_type, symbol, column):
        """
        Returns the cell value for a given strategy_id and symbol for a column

        :param strategy_id: strategy id
        :param product_type: product type
        :param symbol: symbol name
        :param column: column name in positions_df
        :return: value
        """
        try:
            return self._positions_df.get_cell(index=(strategy_id, product_type, symbol), column=column)
        except ValueError:
            return None

    def set_value(self, strategy_id, product_type, symbol, column, value):
        """
        Set the cell value for a given strategy_id and symbol for a column

        :param strategy_id: strategy id
        :param product_type: product type
        :param symbol: symbol name
        :param column: column name in positions_df
        :param value: value to set
        :return: nothing
        """
        if (strategy_id, product_type, symbol) in self._positions_df.index:
            self._positions_df.set_cell((strategy_id, product_type, symbol), column, value)
        else:
            raise ValueError(f'row is not in the positions_df: ({strategy_id}, {product_type}, {symbol})')

    def initialize_row(self, trade):
        """
        Create a new row entry in the positions_df for a strategy_id X symbol combination. Initializes the values to
        the proper levels for adding trades. This needs to be invoked before inserting a trade. If this method is
        invoked and the row already exists then nothing is done, so no harm to call the function in any case.

        :param trade: trade dictionary
        :return: nothing
        """
        if (trade['strategy_id'], trade['product_type'], trade['symbol']) not in self._positions_df.index:
            log.info('Creating new row in positions_df for: {} : {}'.format(trade['strategy_id'], trade['symbol']))
            self._positions_df.set_row(values={'current_position': 0, 'start_position': 0, 'net_quantity': 0,
                                               'buy_quantity': 0, 'sell_quantity': 0, 'buy_avg_price': 0.0,
                                               'sell_avg_price': 0.0, 'buy_pnl': 0.0, 'sell_pnl': 0.0, 'trade_pnl': 0.0,
                                               'position_pnl': 0.0, 'gross_pnl': 0.0, 'commission': 0.0, 'net_pnl': 0.0,
                                               'prior_close_price': None, 'current_price': None},
                                       index=(trade['strategy_id'], trade['product_type'], trade['symbol']))

    @property
    def trade_id(self):
        return self._trade_id

    def new_trade_id(self, increment=True):
        """
        Returns the trade ID for a new trade

        :param increment: If True then increment the internal counter
        :return: trade ID
        """
        trade_id = 1 if self._trade_id is None else self._trade_id + 1
        if increment:
            self._trade_id = trade_id
        return trade_id

    def enter_trade_from_order(self, order):
        """
        Enter trade from Order object

        :param order: Order object
        :return: nothing
        """
        if order.state not in ['PARTIALLY_FILLED', 'FILLED']:
            raise ValueError(f'Order cannot be entered because the state is not FILLED or PARTIALLY_FILLED: {order}')

        fills = order.fills
        unbooked_fills = fills.get(indexes=fills.equality(column='booked', value=False))

        for fill_id in unbooked_fills.index:
            self.enter_trade(order.originator_id, order.strategy_id, unbooked_fills[fill_id, 'bartime'],
                             order.product_type, order.symbol, order.buy_sell, unbooked_fills[fill_id, 'quantity'],
                             unbooked_fills[fill_id, 'price'], commission=unbooked_fills[fill_id, 'commission'],
                             uuid=order.uuid, fill_id=fill_id)
            order.fills.set(fill_id, 'booked', True)
        self._order_manager.set_booked(order, True)
        if order.state == 'FILLED':
            self._order_manager.close_order(order)

    def enter_trade(self, originator_id, strategy_id, bartime, product_type, symbol, buy_sell, quantity, price,
                    **kwargs):
        """
        Enter trade from parameters.

        :param originator_id: originator id
        :param strategy_id: strategy id
        :param bartime: bartime for the trade entry
        :param product_type: product type
        :param symbol: symbol
        :param buy_sell: "buy" or "sell"
        :param quantity: quantity
        :param price: price
        :param kwargs: any other kay / value pairs that will be saved with trade
        :return: nothing
        """
        if buy_sell not in ['buy', 'sell']:
            raise ValueError('buy_sell for trade must be "buy" or "sell"!')

        trade = {'originator_id': originator_id, 'strategy_id': strategy_id, 'bartime': bartime.tz_convert('UTC'),
                 'product_type': product_type, 'symbol': symbol, 'buy_sell': buy_sell, 'quantity': quantity,
                 'price': price}
        trade.update(kwargs)
        log.info(f'Entering trade: {trade}')
        self._insert_trade(trade)
        self._persist_trade(trade)
        self._update_position_df(trade)

    def _insert_trade(self, trade):
        """
        Inserts the trade into the new_trades list of dict.

        :param trade: trade dict
        :return: nothing
        """
        trade['timestamp'] = pd.Timestamp.now(tz='UTC')
        trade['id'] = self.new_trade_id()
        self._new_trades.append(trade)

    def book_fills(self):
        """
        Processes all orders that have unbooked fills. Enters the fills as a trade for all orders.

        :return: dict of orders that were booked for all strategy_ids
        """
        log.info('booking orders fills.')
        orders = self.order_manager.to_be_booked_list()
        booked_orders = {}  # Holds the orders that are processed to be returned at the end
        for order in orders:
            self.enter_trade_from_order(order)
            originator_id = order.originator_id
            if originator_id not in booked_orders:
                booked_orders[originator_id] = []
            booked_orders[originator_id].append(order)
        return booked_orders

    def _update_position_df(self, trade):
        """
        Updates the position_df for the trade. Updates everything except PnL.

        :param trade: trade dictionary
        :return: nothing
        """
        log.info(f'updating row in positions_df for trade: {trade}')
        self.initialize_row(trade)
        strategy_id = trade['strategy_id']
        product_type = trade['product_type']
        symbol = trade['symbol']

        # update the average prices. This must be done first
        avg_px_column = trade['buy_sell'] + '_avg_price'
        total_trades_column = trade['buy_sell'] + '_quantity'
        previous_total_trades = self.get_value(strategy_id, product_type, symbol, total_trades_column)
        previous_avg_px = self.get_value(strategy_id, product_type, symbol, avg_px_column)
        new_avg_px = (previous_avg_px * previous_total_trades + trade['price'] * trade['quantity']) / \
                     (previous_total_trades + trade['quantity'])
        self.set_value(strategy_id, product_type, symbol, avg_px_column, new_avg_px)

        # update the gross buy and sells
        self.set_value(strategy_id, product_type, symbol, total_trades_column,
                       previous_total_trades + trade['quantity'])

        # update the net_trades and current_position
        net_trades = self.get_value(strategy_id, product_type, symbol, 'buy_quantity') - \
                     self.get_value(strategy_id, product_type, symbol, 'sell_quantity')  # noqa
        self.set_value(strategy_id, product_type, symbol, 'net_quantity', net_trades)

        # update current_position
        current_position = self.get_value(strategy_id, product_type, symbol, 'start_position') + \
                           self.get_value(strategy_id, product_type, symbol, 'net_quantity')  # noqa
        self.set_value(strategy_id, product_type, symbol, 'current_position', current_position)

        # update commission
        current_commission = self.get_value(strategy_id, product_type, symbol, 'commission')
        self.set_value(strategy_id, product_type, symbol, 'commission',
                       current_commission + trade.get('commission', 0))

    def _persist_trade(self, trade):
        # persist to a data store
        pass

    def update_pnl(self):
        """
        Runs the complete pnl process by (1) getting the prior day close price for any new position, (2) getting the
        latest live price for all positions and then (3) calculating pnl.

        :return: nothing
        """
        if len(self._positions_df) > 0:
            self.initialize_prior_close()
            self.update_current_prices()
            self.calculate_pnl()

    def initialize_prior_close(self):
        """
        Initialize the prior close price for any new position that does not already have the prior close price. Get
        data from the MarketDataManager and put the result in the positions DataFrame.

        :return: nothing
        """
        strategy_id_position = self.positions_df.index_name.index('strategy_id')
        product_type_position = self.positions_df.index_name.index('product_type')
        symbol_position = self.positions_df.index_name.index('symbol')

        missing_rows = self._positions_df.isin('prior_close_price', [None])
        for row in itertools.compress(self._positions_df.index, missing_rows):
            strategy_id = row[strategy_id_position]
            product_type = row[product_type_position]
            symbol = row[symbol_position]

            # Add the symbols to the MarketDataManager
            self._market_data_manager.add_symbols(product_type, symbol, '1D')

            # Add the symbol at the live frequency to the MarketDataManager and update the data. This is required
            # because the new position may be added after the Runner updates the data for the bar
            self._market_data_manager.add_symbols(product_type, symbol, self._live_frequency)
            self._market_data_manager.update(product_type, self._live_frequency, symbol)

            prior_date = metadb.prior_end_of_day(product_type, symbol, self._market_data_manager.bartime, 1,
                                                 self._metadb)
            self._market_data_manager.load_history(product_type, '1D', symbol, prior_date)
            px = self._market_data_manager.bar(product_type, symbol, '1D', prior_date)['close']
            log.info('Setting the prior close for ({}, {}) to {}'.format(product_type, symbol, str(px)))
            self.set_value(strategy_id, product_type, symbol, 'prior_close_price', px)

    def update_current_prices(self):
        """
        Gets the latest live price from the MarketDataManager for all positions.

        :return: nothing
        """
        log.info(f'updating current prices with frequency: {self._live_frequency}')
        product_type_position = self.positions_df.index_name.index('product_type')
        symbol_position = self.positions_df.index_name.index('symbol')
        new_prices = [self._market_data_manager.last_valid_bar(x[product_type_position], x[symbol_position],
                                                               self._live_frequency)['close'] for
                      x in self._positions_df.index]
        self._positions_df['current_price'] = new_prices

    def insert_today_close(self):
        """
        Update the current_price to today's closing price for all items in the PositionManager. This assumes that the
        data has already been loaded into the MarketDataManager. It takes the current bartime and uses that date to
        determine the closing date, thus it is assumed to be run after the market closes as part of the EOD process.

        :return: nothing
        """
        log.info('inserting todays closing price to current prices.')
        product_type_position = self.positions_df.index_name.index('product_type')
        symbol_position = self.positions_df.index_name.index('symbol')
        close_prices = [
            self._market_data_manager.current_bar(x[product_type_position], x[symbol_position], '1D')['close']
            for x in self._positions_df.index]
        self._positions_df['current_price'] = close_prices

    def calculate_pnl(self):
        """
        Calculate the PnL for all positions.

        :return: nothing
        """
        log.info('calculating pnl')

        self._positions_df['buy_pnl'] = \
            cutils.element_math(self._positions_df.get_entire_column('buy_quantity', as_list=True),
                                self._positions_df.subtract('prior_close_price', 'buy_avg_price'), operator.mul)

        self._positions_df['sell_pnl'] = \
            cutils.element_math(self._positions_df.get_entire_column('sell_quantity', as_list=True),
                                self._positions_df.subtract('sell_avg_price', 'prior_close_price'), operator.mul)

        self._positions_df['trade_pnl'] = self._positions_df.add('buy_pnl', 'sell_pnl')

        self._positions_df['position_pnl'] = \
            cutils.element_math(self._positions_df.get_entire_column('current_position', as_list=True),
                                self._positions_df.subtract('current_price', 'prior_close_price'), operator.mul)

        self._positions_df['gross_pnl'] = self._positions_df.add('trade_pnl', 'position_pnl')

        self._positions_df['net_pnl'] = self._positions_df.add('gross_pnl', 'commission')

    def stop(self):
        """
        Run the stop processes

        :return: nothing
        """
        log.info('running stop process')
        self.update_pnl()
        if len(self._positions_df) > 0:
            self.save_positions(self._market_data_manager.bartime)
        self.save_positions_df(self._market_data_manager.bartime)
        self.calculate_eod_metrics(self._market_data_manager.bartime)

    def begin_of_day(self):
        """
        Run the begin of day processes

        :return: nothing
        """
        log.info('running BOD process')
        self.load_positions(tapdb.max_datetime(self._tapdb, source=self.id))
        self.initialize_prior_close()

    def end_of_day(self):
        """
        Run the end of day processes

        :return: nothing
        """
        log.info('running EOD process')
        datetime = self._market_data_manager.bartime
        if len(self._positions_df) > 0:
            self.insert_today_close()
            self.calculate_pnl()
            self.calculate_eod_metrics(datetime)
            self.save_positions(datetime)
        self.save_positions_df(datetime)

    def save_positions(self, datetime):
        """
        Save positions to the database (connected by the tapdb_engine)

        :param datetime: datetime of the save
        :return: nothing
        """

        log.info('saving positions to TAPDB')
        log.info('converting positions_df to pandas dataframe for insert')
        positions = copy.deepcopy(self.positions_df)  # so as not to alter existing internal positions_df
        positions.reset_index()
        positions_pd = pdutils.rc_to_pd(positions[['strategy_id', 'product_type', 'symbol', 'current_position']])
        # rename columns and create new needed columns
        positions_pd = positions_pd.rename(columns={'strategy_id': 'strategy', 'current_position': 'position'})
        positions_pd['datetime'] = datetime
        positions_pd['source'] = self.id

        log.info(f'insert positions dataframe to tapdb for datetime: {datetime}')
        tapdb.insert_positions(self._tapdb, positions_pd)

    def load_positions(self, datetime):
        """
        Load positions from TAPDB for a given datetime and overwrite existing positions_df with these positions.

        :param datetime: datetime to load from database
        :return: nothing
        """
        log.info('loading positions')
        # load positions from the database
        self._reset_positions_df()
        log.info('getting positions from TAPDB')
        db_positions = tapdb.get_positions(self._tapdb, source=self.id, datetime=datetime)

        for row in db_positions.itertuples():
            if row.position != 0:  # do not bother to insert rows with zero starting position
                new_row = {'strategy_id': row.strategy, 'product_type': row.product_type, 'symbol': row.symbol}
                log.info('inserting position from TAPDB into positions_df', new_row)
                self.initialize_row(new_row)
                self.set_value(row.strategy, row.product_type, row.symbol, 'start_position', row.position)
                self.set_value(row.strategy, row.product_type, row.symbol, 'current_position', row.position)

    def save_positions_df(self, datetime):
        """
        Save positions_df to the database (connected by the tapdb_engine)

        :param datetime: datetime of the save
        :return: nothing
        """
        log.info('saving positions_df DataFrame to TAPDB')
        tapdb.insert_positions_df(self._tapdb, self.id, datetime, self.positions_df)

    def add_eod_metric(self, metric, metric_id):
        """
        Adds a Metric to the list of metrics to be calculated as part of the end of day process. The order that the
        metrics are added will be the order that they are evaluated, so if there are dependencies be sure to add in the
        correct order.

        :param metric: Metric object
        :param metric_id: metric ID or name
        :return: nothing
        """
        self._eod_metrics[metric_id] = metric

    @property
    def eod_metrics(self):
        return self._eod_metrics

    def calculate_eod_metrics(self, datetime):
        """
        Calculates all of the end of day metrics for a given datetime.

        :param datetime: datetime for the calculation
        :return: nothing
        """
        for metric in self._eod_metrics.values():
            metric.calculate(datetime)
