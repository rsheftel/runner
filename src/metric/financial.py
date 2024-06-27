"""
Financial metrics like PnL, Sharpe, etc
"""

from .metric import Metric


class PositionManagerMetric(Metric):
    """
    Generic financial metric that operates on the PositionManager data.
    """

    def __init__(self, market_data_manager, position_manager, column, aggregation_fn,
                 strategy_id=None, product_type=None, symbol=None):
        """

        :param market_data_manager: MarketDataManager object
        :param position_manager: PositionManager object
        :param column: column from the PositionManager DataFrame
        :param aggregation_fn: aggregation function to apply to the list
        :param strategy_id: strategy ID, or None for all
        :param product_type: product type, or None for all
        :param symbol: symbol name or None for all
        """
        super().__init__(market_data_manager)
        self._column = column
        self._aggregation_fn = aggregation_fn
        self._strategy_id = strategy_id
        self._product_type = product_type
        self._symbol = symbol
        self.df = position_manager.positions_df

    def _calculate(self, datetime):
        rows = self.df.select_index((self._strategy_id, self._product_type, self._symbol))
        return self._aggregation_fn(self.df.get_rows(rows, self._column, as_list=True))
