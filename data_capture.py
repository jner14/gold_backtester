from __future__ import print_function

import data_processing
import datetime
import sqlite3 as lite
import urllib
from datetime import timedelta, date
from bs4 import BeautifulSoup


class Ticker(object):

    DATE_FMT = '%Y-%m-%d'
    TIME_FMT = '%H:%M:%S'

    def __init__(self):
        self.symbol = ''
        self.db_path = ''
        self.date, self.time, self.date_time, self.open_, self.high, self.low, self.close, self.volume, self.adj_close \
            = ([] for _ in range(9))

    def append(self, dt, open_, high, low, close, volume, adj_close):
        self.date.append(dt.date())
        self.time.append(dt.time())
        self.date_time.append(dt)
        self.open_.append(float(open_))
        self.high.append(float(high))
        self.low.append(float(low))
        self.close.append(float(close))
        self.volume.append(int(volume))
        self.adj_close.append(float(adj_close))

    def to_csv(self):
        return ''.join(["{0},{1},{2},{3:.2f},{4:.2f},{5:.2f},{6:.2f},{7},{8:.2f}\n".format(self.symbol,
                                                                                           self.date[bar].strftime(
                                                                                               '%Y-%m-%d'),
                                                                                           self.time[bar].strftime(
                                                                                               '%H:%M:%S'),
                                                                                           self.open_[bar],
                                                                                           self.high[bar],
                                                                                           self.low[bar],
                                                                                           self.close[bar],
                                                                                           self.volume[bar],
                                                                                           self.adj_close[bar])
                        for bar in range(len(self.close))])

    def write_csv(self, filename):
        with open(filename, 'w') as f:
            f.write(self.to_csv())

    def read_csv(self, filename):
        self.symbol = ''
        self.date, self.time, self.date_time, self.open_, self.high, self.low, self.close, self.volume = ([] for _ in
                                                                                                          range(8))
        for line in open(filename, 'r'):
            symbol, ds, ts, open_, high, low, close, volume = line.rstrip().split(',')
            self.symbol = symbol
            dt = datetime.datetime.strptime(ds + ' ' + ts, self.DATE_FMT + ' ' + self.TIME_FMT)
            self.append(dt, open_, high, low, close, volume)
        return True

    def read_yahoo_csv(self, csv_path, ticker):
        self.symbol = ''
        self.date, self.time, self.date_time, self.open_, self.high, self.low, self.close, self.volume, self.adj_close = (
        [] for _ in range(9))
        f = open(csv_path, 'r')
        f.next()
        for line in f:
            ds, open_, high, low, close, volume, adj_close = line.rstrip().split(',')
            self.symbol = ticker
            ts = '0:00:00'
            dt = datetime.datetime.strptime(ds + ' ' + ts, self.DATE_FMT + ' ' + self.TIME_FMT)
            self.append(dt, open_, high, low, close, volume, adj_close)
        return True

    #    def read_db(self,db_path,ticker):
    ##        ticker_ = '[' + ticker + ']'
    #        self.symbol = ''
    #        self.date,self.time,self.date_time,self.open_,self.high,self.low,self.close,self.volume = ([] for _ in range(8))
    #        con = lite.connect(db_path)
    #        try:
    #            with con:
    #                cur = con.cursor()
    #                cur.execute('SELECT * FROM [%s] ORDER BY Datetime ASC' % ticker)
    #                rows = cur.fetchall()
    #                self.symbol = ticker
    #                self.db_path = db_path
    #                for row in rows:
    #                    dt,open_,close,high,low,volume = row[0],row[1],row[2],row[3],row[4],row[5]
    #                    self.append(self.read_dt(dt),open_,high,low,close,volume)
    #        except:
    #            return False
    #        return True

    def read_last_quote(self, db_path, ticker):
        ticker_ = '[' + ticker + ']'
        self.symbol = ''
        self.date, self.time, self.date_time, self.open_, self.high, self.low, self.close, self.volume, self.adj_close = (
        [] for _ in range(9))
        con = lite.connect(db_path)
        try:
            with con:
                cur = con.cursor()
                cur.execute('SELECT * FROM %s ORDER BY Datetime DESC LIMIT 1' % ticker_)
                lq = cur.fetchone()
                dt, open_, close, high, low, volume, adj_close = lq[0], lq[1], lq[2], lq[3], lq[4], lq[5], lq[6]
                self.symbol = ticker
                self.db_path = db_path
                self.append(self.read_dt(dt), open_, high, low, close, volume, adj_close)
                #            print(self.date_time,self.open_,self.close,self.high,self.low,self.volume)
        except:
            return False
        return True

    # convert the date time 2015_05_01_13_00_00 to a datetime object
    def read_dt(self, dt):
        if dt.count('_') == 2:
            year, month, day = dt.rstrip().split('_')
            return datetime.datetime(int(year), int(month), int(day))
        else:
            year, month, day, hour, minute, second = dt.rstrip().split('_')
            return datetime.datetime(int(year), int(month), int(day),
                                     int(hour), int(minute), int(second))

    def overwrite_db(self, db_path=None):
        f = '{0:02d}'
        if db_path == None:
            if 'daily' in self.db_path:
                intraday = False
            elif 'minutes' in self.db_path:
                intraday = True
            else:
                return False
        else:
            self.db_path = db_path
            intraday = False
        ticker = '[' + self.symbol + ']'
        con = lite.connect(self.db_path)
        with con:
            stockvalues = []
            for i in range(len(self.date_time)):
                d = self.date[i]
                t = self.time[i]
                if intraday:
                    dt_int = (str(d.year) + '_' + f.format(d.month) + '_' +
                              f.format(d.day) + '_' + f.format(t.hour) + '_' +
                              f.format(t.minute) + '_' + f.format(t.second))
                else:
                    dt_int = (str(d.year) + '_' + f.format(d.month) + '_' + f.format(d.day))

                stockvalues.append((dt_int,
                                    self.open_[i],
                                    self.close[i],
                                    self.high[i],
                                    self.low[i],
                                    self.volume[i],
                                    self.adj_close[i]))
            stockvalues = tuple(stockvalues)
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS %s" % ticker)
            cur.execute(
                "CREATE TABLE %s(Datetime INT PRIMARY KEY, Open REAL, Close REAL, High REAL, Low REAL, Volume INT, Adj_Close REAL)" % ticker)
            sqlite_string = "INSERT INTO %s VALUES(?, ?, ?, ?, ?, ?, ?)" % ticker
            cur.executemany(sqlite_string, stockvalues)

        #            cur.execute("SELECT * FROM %s" % ticker)

        #            while True:
        #                row = cur.fetchone()
        #                if row == None:
        #                    break
        #                print row[0], row[1], row[2],row[3], row[4], row[5]

    def update_db(self):
        f = '{0:02d}'
        if 'daily' in self.db_path:
            intraday = False
        elif 'minutes' in self.db_path:
            intraday = True
        else:
            return False
        ticker = '[' + self.symbol + ']'
        con = lite.connect(self.db_path)
        with con:
            stockvalues = []
            while len(self.open_) > 0:
                d = self.date.pop(0)
                t = self.time.pop(0)
                self.date_time.pop(0)
                if intraday:
                    dt_int = (str(d.year) + '_' + f.format(d.month) + '_' +
                              f.format(d.day) + '_' + f.format(t.hour) + '_' +
                              f.format(t.minute) + '_' + f.format(t.second))
                else:
                    dt_int = (str(d.year) + '_' + f.format(d.month) + '_' + f.format(d.day))

                stockvalues.append((dt_int,
                                    self.open_.pop(0),
                                    self.close.pop(0),
                                    self.high.pop(0),
                                    self.low.pop(0),
                                    self.volume.pop(0),
                                    self.adj_close.pop(0)))
            stockvalues = tuple(stockvalues)
            cur = con.cursor()
            sqlite_string = "INSERT INTO %s VALUES(?, ?, ?, ?, ?, ?, ?)" % ticker
            result = cur.executemany(sqlite_string, stockvalues)
            pass

        #            cur.execute("SELECT * FROM %s" % ticker)

        #            while True:
        #                row = cur.fetchone()
        #                if row == None:
        #                    break
        #                print row[0], row[1], row[2],row[3], row[4], row[5]

    def delete_quote_by_index(self, i):
        self.date_time.pop(i)
        self.date.pop(i)
        self.time.pop(i)
        self.open_.pop(i)
        self.close.pop(i)
        self.high.pop(i)
        self.low.pop(i)
        self.volume.pop(i)
        self.adj_close.pop(i)
        self.overwrite_db()
        return True

    def get_quote_by_index(self, i):
        quote = (self.date_time[i], self.open_[i], self.close[i],
                 self.high[i], self.low[i], self.volume[i], self.adj_close[i])
        return quote

    def __repr__(self):
        return self.to_csv()


class IntradayQuotes(Ticker):
    """ Intraday quotes from Google. Specify interval seconds and number of days """

    def __init__(self, symbol, interval_seconds=300, num_days=5):
        super(IntradayQuotes, self).__init__()
        self.symbol = symbol.upper()
        self.db_path = 'data/minutes_1.db'
        url_string = "http://www.google.com/finance/getprices?q={0}".format(self.symbol)
        url_string += "&i={0}&p={1}d&f=d,o,h,l,c,v".format(interval_seconds, num_days)
        if interval_seconds < 60:
            s_o = 60 / interval_seconds  # special offset for less than 60s intervals
        else:
            s_o = 1
        csv = urllib.urlopen(url_string).readlines()
        for bar in range(7, len(csv)):
            if csv[bar].count(',') != 5: continue
            offset, close, high, low, open_, volume = csv[bar].split(',')
            if offset[0] == 'a':
                day = float(offset[1:])
                offset = 0
            else:
                offset = float(offset)
            open_, high, low, close = [float(x) for x in [open_, high, low, close]]
            dt = datetime.datetime.fromtimestamp(day + (interval_seconds * offset / s_o))
            adj_close = 0
            self.append(dt, open_, high, low, close, volume, adj_close)


class DailyQuotes(Ticker):
    """ Daily quotes from Yahoo or Google. Date format='yyyy-mm-dd' """

    def __init__(self,
                 symbol,
                 start_date = datetime.datetime(1970, 1, 1),
                 end_date   = datetime.date.today().isoformat(),
                 source     = 'yahoo',
                 db_path    = 'data/daily_gold.db'):

        super(DailyQuotes, self).__init__()
        self.symbol = symbol.upper()
        self.db_path = 'data/daily_gold.db'
        if type(start_date) == datetime.date:
            start = start_date
        else:
            start = datetime.date(int(start_date[0:4]), int(start_date[5:7]), int(start_date[8:10]))

        end = datetime.date(int(end_date[0:4]), int(end_date[5:7]), int(end_date[8:10]))
        if source == 'yahoo':
            url_string = 'http://real-chart.finance.yahoo.com/table.csv?s=%s' % self.symbol
            url_string += '&a=%s&b=%s&c=%s&' % (start.month, start.day, start.year)
            url_string += 'd=%s&e=%s&f=%s&g=d&ignore=.csv' % (end.month, end.day, end.year)
        elif source == 'google':
            url_string = "http://www.google.com/finance/historical?q={0}".format(self.symbol)
            url_string += "&startdate={0}&enddate={1}&output=csv".format(
                start.strftime('%b %d, %Y'), end.strftime('%b %d, %Y'))
        else:
            print("Must select 'yahoo' or 'google' for source!")
            return

        csv = urllib.urlopen(url_string).readlines()
        csv.reverse()
        if 'head' not in csv and '</div></body></html>\n' not in csv:
            if source == 'yahoo':
                for bar in range(0, len(csv) - 1):
                    ds, open_, high, low, close, volume, adj_close = csv[bar].rstrip().split(',')
                    ds = ds.rstrip().split('-')
                    open_, high, low, close, volume, adj_close = [x.replace('-', '0') for x in [open_, high, low, close,
                                                                                                volume, adj_close]]
                    open_, high, low, close, adj_close = [float(x) for x in [open_, high, low, close, adj_close]]
                    dt = datetime.datetime(int(ds[0]), int(ds[1]), int(ds[2]))
                    self.append(dt, open_, high, low, close, volume, adj_close)
            elif source == 'google':
                for bar in range(0, len(csv) - 1):
                    ds, open_, high, low, close, volume = csv[bar].rstrip().split(',')
                    open_, high, low, close, volume = [x.replace('-', '0') for x in [open_, high, low, close, volume]]
                    open_, high, low, close = [float(x) for x in [open_, high, low, close]]
                    dt = datetime.datetime.strptime(ds, '%d-%b-%y')
                    self.append(dt, open_, high, low, close, volume)
        elif 'Sorry' in ''.join(csv):
            print("#" * 60)
            print("YAHOO RETURNED THE FOLLOWING 404 ERROR: Sorry, the page you requested was not found.")
            print("#" * 60)


### Works with YAHOO only
def download_latest_quotes(type_='daily', time_frame=360):
    empties = []

    if type_ == 'daily':
        time_cat = type_
        if time_frame < 60: time_frame = 60  # yahoo needs atleast 60 days
        time_frame = date.today() - timedelta(days=time_frame)
    elif type_ == 'minutes':
        interval = 60
        interval_print = interval / 60
        time_cat = type_
    else:
        print("Quote type error! Use 'daily' or 'minutes'.")
        return False

    if type_ == 'daily':
        print("Begining %s..." % time_cat)
    else:
        print("Begining %s %s..." % (interval, time_cat))

    tickers, rand_state = data_processing.load_tickers()
    for ticker in tickers:
        t1 = Ticker()
        spaces = abs(len(ticker) - 5) * ' '
        tickerPrint = ticker + spaces

        print("-\n%s>> " % tickerPrint),
        print("downloading... "),
        if type_ == 'daily':
            t2 = DailyQuotes(ticker, time_frame)
        else:
            t2 = IntradayQuotes(ticker, interval, time_frame)

        if len(t2.date) == 0:
            empties.append(t2.symbol)
            print("DATA MISSING!!!")
            continue

        print("checking ours... "),
        if type_ == 'daily':
            path = ("data/%s.db" % time_cat)
        else:
            path = ("data/%s_%s.db" % (time_cat, interval_print))
        if t1.read_last_quote(path, ticker):
            # we don't load everything from the ticker only the last quote into t1
            # we then only pop the quote in t2 if it is older or equal to the last quote in t1
            # When we find the first new quote in t2 we stop and run update_db on t2
            print("updating..."),
            additions = 0
            while len(t2.date) > 0:
                if t2.date_time[0] > t1.date_time[-1]:
                    additions = len(t2.date)
                    t2.update_db()
                    break
                else:
                    t2.date.pop(0)
                    t2.time.pop(0)
                    t2.date_time.pop(0)
                    t2.open_.pop(0)
                    t2.close.pop(0)
                    t2.high.pop(0)
                    t2.low.pop(0)
                    t2.volume.pop(0)
                    t2.adj_close.pop(0)
            print("%s... " % additions),
        else:  # ticker table doesn't exist so download all history available
            if type_ == 'daily':
                t2 = DailyQuotes(ticker, '1970-01-01')
            else:
                t2 = IntradayQuotes(ticker, interval, 360)
            print("new ticker added... "),
            t2.overwrite_db()
        print("COMPLETE!!")
    print("These were not downloaded: %s" % empties)


# download_latest_quotes('daily', 180)
# download_latest_quotes('minutes')


if __name__ == '__main__':

    # Run this to update quote database with latest quotes
    from data_processing import load_tickers

    db_path = 'data/daily_gold.db'
    picks_path = 'symbols/gold_picks.csv'
    gdx_path = 'symbols/gold_gdx.csv'
    create_from_scratch = True

    # Load tickers from file
    picks_tickers, rand_state = load_tickers(validate=False, db_path=db_path, ticker_path=picks_path, min_samples=1)
    gdx_tickers, rand_state = load_tickers(validate=False, db_path=db_path, ticker_path=gdx_path, min_samples=1)
    all_tickers = set(picks_tickers + gdx_tickers + ['SPY', 'GDX'])

    # Download quotes then either update or create tables in db
    print("Downloading Stock Prices!")
    for t in all_tickers:

        # create ticker DailyQuotes object and download quotes
        t1 = DailyQuotes(symbol     = t,
                         start_date = '2007-01-01',  # '2007-09-23'  '2016-11-1'
                         db_path    = db_path)

        print("%s: %s" % (t, len(t1.date)))

        # If quotes were downloaded for the given ticker, create or update the ticker's db table
        if len(t1.date) > 1:
            if create_from_scratch:
                t1.overwrite_db()
                print("Table %s has been deleted and recreated in database %s." % (t, db_path))
            else:
                try:
                    t1.update_db()
                    print("Table %s has been updated in database %s." % (t, db_path))
                except:
                    t1.overwrite_db()
                    print("Table %s has been created in database %s." % (t, db_path))

    ### A DailyQuotes object downloads quote data from yahoo during init
    ##t1 = DailyQuotes('SPY','2007-01-01')
    ##t1.overwrite_db()

    print("DONE")
