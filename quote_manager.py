import sqlite3 as lite
from pandas import DataFrame, read_sql_query


class QuoteManager(object):
    """Interacts with database of stock prices."""

    QUOTE_TYPES = ['Open', 'Close', 'High', 'Low', 'Volume']


    def __init__(self, db_path):
        self.db_path = db_path
        self.con = lite.connect(self.db_path)
        self._quotes = {}

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


    def get_quote(self, symbol, date, type='Close'):
        # Assert that the quote type passed is valid
        assert type in self.QUOTE_TYPES, \
            "ERROR in QuoteManager.get_quote() >> %s is not in %s" % (type, self.QUOTE_TYPES)

        # If there is not quote data for a given symbol return nan
        if symbol == 'ANVGQ':
            print("Quote data is not available for %s" % symbol)
            return 'nan'

        # If there is quote data available for the given date, return it
        if date in self._quotes[symbol][type].index:
            return self._quotes[symbol][type][date]
        else:
            print("Quote data is not available on %s for %s" % (date, symbol))
            return 'nan'


# Used for debugging and development
if __name__ == '__main__':
    qm = QuoteManager('data/daily_gold.db')
    for symbol in qm._quotes:
        print(qm.get_quote(symbol, '2016_10_17'))
        pass
