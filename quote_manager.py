import sqlite3 as lite
from pandas import DataFrame, read_sql_query
import numpy as np


class QuoteManager(object):
    """Interacts with database of stock prices."""

    QUOTE_TYPES = ['Open', 'Close', 'High', 'Low', 'Volume', 'Adj_Close', 'All']

    def __init__(self, db_path, debugger):
        self.db_path = db_path
        self.con = lite.connect(self.db_path)
        self._quotes = {}
        self.to_console = debugger.to_console

        # Connect to quotes database and load all quotes into memory
        # TODO: If using files larger than 10mb, consider modifying to optimize memory usage
        with self.con:
            cur = self.con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cur.fetchall()
            for table in tables:
                df = read_sql_query("SELECT * from [%s]" % table[0], self.con)
                self._quotes[str(table[0])] = df.set_index('Datetime')
                pass
        print("QuoteManager has the database %s loaded into memory..." % db_path)
        return super(QuoteManager, self).__init__()

    def get_quote(self, symbol, date, type='Adj_Close'):
        # Assert that the quote type passed is valid
        assert type in self.QUOTE_TYPES, \
            "ERROR in QuoteManager.get_quote() >> %s is not in %s" % (type, self.QUOTE_TYPES)

        # If there is not quote data for a given symbol return nan
        if symbol == 'ANVGQ':
            self.to_console("Quote data is not available for %s" % symbol)
            return np.nan

        # If there is quote data available for the given date, return it
        if date in self._quotes[symbol][type].index:
            return self._quotes[symbol][type][date]
        else:
            self.to_console("Quote data is not available on %s for %s" % (date, symbol))
            return np.nan

    def get_prev_date(self, symbol, date):
        # If there is a previous date available for the given date, return it
        if date in self._quotes[symbol].index:
            i = self._quotes[symbol].index.get_loc(date)
            if i > 0:
                return self._quotes[symbol].iloc[i-1].name
            else:
                self.to_console("Quote data is not available on day before %s for %s" % (date, symbol))
                return np.nan
        else:
            self.to_console("Quote data is not available on %s for %s" % (date, symbol))
            return np.nan

    def get_quote_range(self, symbol, date, days, fields=['Adj_Close']):
        # Assert that the quote type passed is valid
        for field in fields:
            assert field in self.QUOTE_TYPES, \
                "ERROR in QuoteManager.get_quote() >> %s is not in %s" % (fields, self.QUOTE_TYPES)

        # If type = All, return all fields
        if 'All' in fields:
            fields = ['Open', 'Close', 'High', 'Low', 'Volume', 'Adj_Close']

        # If there is quote data available for the needed date, return it
        if date in self._quotes[symbol].index:
            i = self._quotes[symbol].index.get_loc(date)

            if i - days - 1 >= 0:
                return self._quotes[symbol].ix[i-days-1:i, fields]
            else:
                return None
        else:
            self.to_console("Quote data is not available on %s for %s" % (date, symbol))
            return np.nan

    def get_atr(self, symbol, date, span):
        # Ensure date and previous dates of span exist
        endI = 0
        if date in self._quotes[symbol].index:
            endI = self._quotes[symbol].index.get_loc(date)

        startI = endI - span - 1
        quotes = None
        if startI >= 0:
            quotes = self._quotes[symbol].iloc[startI:endI].copy()
        else:
            result = None

        if quotes is not None:
            quotes['prev'] = quotes.Close.shift()
            quotes.drop(quotes.index[0], inplace=True)
            quotes['tdr1'] = quotes.High - quotes.Low
            quotes['tdr2'] = (quotes.High - quotes.prev).abs()
            quotes['tdr3'] = (quotes.Low - quotes.prev).abs()
            quotes['tdr'] = quotes[['tdr1', 'tdr2', 'tdr3']].max(axis=1)

            result = quotes.tdr.mean()

        return result


# Used for debugging and development
if __name__ == '__main__':
    qm = QuoteManager('data/daily_gold.db')
    for s in qm._quotes:
        print(qm.get_quote(s, '2016_10_17'))
        pass
