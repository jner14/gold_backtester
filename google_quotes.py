# Copyright (c) 2011, Mark Chenoweth
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted 
# provided that the following conditions are met:
#
# - Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
#
# - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following 
#   disclaimer in the documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, 
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE 
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS 
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF 
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import urllib,datetime,os,sqlite3 as lite

class Ticker(object):
    
    DATE_FMT = '%Y-%m-%d'
    TIME_FMT = '%H:%M:%S'
  
    def __init__(self):
        self.symbol = ''
        self.db_path = ''
        self.date,self.time,self.date_time,self.open_,self.high,self.low,self.close,self.volume = ([] for _ in range(8))

    def append(self,dt,open_,high,low,close,volume):
        self.date.append(dt.date())
        self.time.append(dt.time())
        self.date_time.append(dt)
        self.open_.append(float(open_))
        self.high.append(float(high))
        self.low.append(float(low))
        self.close.append(float(close))
        self.volume.append(int(volume))
      
    def to_csv(self):
        return ''.join(["{0},{1},{2},{3:.2f},{4:.2f},{5:.2f},{6:.2f},{7}\n".format(self.symbol,
                        self.date[bar].strftime('%Y-%m-%d'),self.time[bar].strftime('%H:%M:%S'),
                        self.open_[bar],self.high[bar],self.low[bar],self.close[bar],self.volume[bar]) 
                        for bar in xrange(len(self.close))])
    
    def write_csv(self,filename):
        with open(filename,'w') as f:
            f.write(self.to_csv())
   
    def read_csv(self,filename):
        self.symbol = ''
        self.date,self.time,self.date_time,self.open_,self.high,self.low,self.close,self.volume = ([] for _ in range(8))
        for line in open(filename,'r'):
            symbol,ds,ts,open_,high,low,close,volume = line.rstrip().split(',')
            self.symbol = symbol
            dt = datetime.datetime.strptime(ds+' '+ts,self.DATE_FMT+' '+self.TIME_FMT)
            self.append(dt,open_,high,low,close,volume)
        return True
    
    def read_yahoo_csv(self,csv_path,ticker):
        self.symbol = ''
        self.date,self.time,self.date_time,self.open_,self.high,self.low,self.close,self.volume = ([] for _ in range(8))
        f = open(csv_path,'r')
        f.next()
        for line in f:
            ds,open_,high,low,close,volume,adj_close = line.rstrip().split(',')
            self.symbol = ticker
            ts = '0:00:00'
            dt = datetime.datetime.strptime(ds+' '+ts,self.DATE_FMT+' '+self.TIME_FMT)
            self.append(dt,open_,high,low,close,volume)
        return True

    def read_db(self,db_path,ticker):
#        ticker_ = '[' + ticker + ']'
        self.symbol = ''
        self.date,self.time,self.date_time,self.open_,self.high,self.low,self.close,self.volume = ([] for _ in range(8))
        con = lite.connect(db_path)
        try:
            with con:
                cur = con.cursor()
                cur.execute('SELECT * FROM [%s] ORDER BY Datetime ASC' % ticker)
                rows = cur.fetchall()
                self.symbol = ticker
                self.db_path = db_path
                for row in rows:
                    dt,open_,close,high,low,volume = row[0],row[1],row[2],row[3],row[4],row[5]
                    self.append(self.read_dt(dt),open_,high,low,close,volume)
        except:
            return False
        return True
            
    def read_last_quote(self,db_path,ticker):
        ticker_ = '[' + ticker + ']'
        self.symbol = ''
        self.date,self.time,self.date_time,self.open_,self.high,self.low,self.close,self.volume = ([] for _ in range(8))
        con = lite.connect(db_path)
        try:
            with con:
                cur = con.cursor()
                cur.execute('SELECT * FROM %s ORDER BY Datetime DESC LIMIT 1' % ticker_)
                lq = cur.fetchone()
                dt,open_,close,high,low,volume = lq[0],lq[1],lq[2],lq[3],lq[4],lq[5]
                self.symbol = ticker
                self.db_path = db_path
                self.append(self.read_dt(dt),open_,high,low,close,volume)
    #            print(self.date_time,self.open_,self.close,self.high,self.low,self.volume)
        except:
            return False
        return True
            
    #convert the date time 2015_05_01_13_00_00 to a datetime object
    def read_dt(self,dt):
        if dt.count('_') == 2:
            year,month,day = dt.rstrip().split('_')
            return datetime.datetime(int(year), int(month),  int(day))
        else:
            year,month,day,hour,minute,second = dt.rstrip().split('_')
            return datetime.datetime(int(year), int(month),  int(day),
                                     int(hour), int(minute), int(second))
        
    def overwrite_db(self,db_path=None):
        f = '{0:02d}'
        if db_path==None:
            if   'daily'   in self.db_path: intraday = False
            elif 'minutes' in self.db_path: intraday = True
            else: return False
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
                                    self.volume[i]))
            stockvalues = tuple(stockvalues)
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS %s" % ticker)
            cur.execute("CREATE TABLE %s(Datetime INT, Open REAL, Close REAL, High REAL, Low REAL, Volume INT)" % ticker)
            sqlite_string = "INSERT INTO %s VALUES(?, ?, ?, ?, ?, ?)" % ticker
            cur.executemany(sqlite_string, stockvalues)
            
#            cur.execute("SELECT * FROM %s" % ticker)

#            while True:
#                row = cur.fetchone()
#                if row == None:
#                    break
#                print row[0], row[1], row[2],row[3], row[4], row[5]

    def update_db(self):
        f = '{0:02d}'
        if   'daily'   in self.db_path: intraday = False
        elif 'minutes' in self.db_path: intraday = True
        else:
            print("db_path does not contain 'daily' or 'minutes'. Update cancelled!") 
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
                                    self.volume.pop(0)))
            stockvalues = tuple(stockvalues)
            cur = con.cursor()
            sqlite_string = "INSERT INTO %s VALUES(?, ?, ?, ?, ?, ?)" % ticker
            cur.executemany(sqlite_string, stockvalues)
            
#            cur.execute("SELECT * FROM %s" % ticker)

#            while True:
#                row = cur.fetchone()
#                if row == None:
#                    break
#                print row[0], row[1], row[2],row[3], row[4], row[5]
    
    def delete_quote_by_index(self,i):
        self.date_time.pop(i)
        self.date.pop(i)
        self.time.pop(i)
        self.open_.pop(i)
        self.close.pop(i)
        self.high.pop(i)
        self.low.pop(i)
        self.volume.pop(i)
        self.overwrite_db()
        return True
        
    def get_quote_by_index(self,i):
        quote = (self.date_time[i], self.open_[i], self.close[i], 
                 self.high[i],      self.low[i],   self.volume[i])
        return quote
        
    def __repr__(self):
        return self.to_csv()

class IntradayQuotes(Ticker):
  ''' Intraday quotes from Google. Specify interval seconds and number of days '''
  def __init__(self,symbol,interval_seconds=300,num_days=5):
    super(IntradayQuotes,self).__init__()
    self.symbol = symbol.upper()
    self.db_path = 'data/minutes_1.db'
    url_string = "http://www.google.com/finance/getprices?q={0}".format(self.symbol)
    url_string += "&i={0}&p={1}d&f=d,o,h,l,c,v".format(interval_seconds,num_days)
    if interval_seconds < 60:
        s_o = 60 / interval_seconds #special offset for less than 60s intervals
    else:
        s_o = 1
    csv = urllib.urlopen(url_string).readlines()
    for bar in xrange(7,len(csv)):
      if csv[bar].count(',')!=5: continue
      offset,close,high,low,open_,volume = csv[bar].split(',')
      if offset[0]=='a':
        day = float(offset[1:])
        offset = 0
      else:
        offset = float(offset)
      open_,high,low,close = [float(x) for x in [open_,high,low,close]]
      dt = datetime.datetime.fromtimestamp(day+(interval_seconds*offset / s_o))
      self.append(dt,open_,high,low,close,volume)

class DailyQuotes(Ticker):
    ''' Daily quotes from Yahoo or Google. Date format='yyyy-mm-dd' '''
    def __init__(self,symbol,
                 start_date=datetime.date(1970,1,1),
                 end_date=datetime.date.today().isoformat(),
                 source='yahoo',
                 db_path='data/daily.db'):
                     
        super(DailyQuotes, self).__init__()
        self.symbol = symbol.upper()
        self.db_path = db_path

        if type(start_date) == datetime.date:
            start = start_date
        else:
            start = datetime.date(int(start_date[0:4]), int(start_date[5:7]), int(start_date[8:10]))
        
        if type(end_date) == datetime.date:
            end = end_date
        else:
            end = datetime.date(int(end_date[0:4]), int(end_date[5:7]), int(end_date[8:10]))

        if source == 'yahoo':
            url_string = 'http://real-chart.finance.yahoo.com/table.csv?s=%s' % self.symbol
            url_string += '&a=%s&b=%s&c=%s&' % (start.month, start.day, start.year)
            url_string += 'd=%s&e=%s&f=%s&g=d&ignore=.csv' % (end.month, end.day, end.year)
        elif source == 'google':
            url_string = "http://www.google.com/finance/historical?q={0}".format(self.symbol)
            url_string += "&startdate={0}&enddate={1}&output=csv".format(
                          start.strftime('%b %d, %Y'),end.strftime('%b %d, %Y'))
        else:
            print("Must select 'yahoo' or 'google' for source!")
            return
        
        csv = urllib.urlopen(url_string).readlines()
        csv.reverse()
        if 'head' not in csv and '</div></body></html>\n' not in csv:
#            print("DOWNLOADED GARBAGE\n%s\n%s\n%s" %('='*88, csv, '='*88))
#        else:
            if source == 'yahoo':
                for bar in xrange(0,len(csv)-1):
                    ds,open_,high,low,close,volume,adj_close = csv[bar].rstrip().split(',')
                    ds = ds.rstrip().split('-')
                    open_,high,low,close,volume = [x.replace('-', '0') for x in [open_,high,low,close,volume]]
                    open_,high,low,close = [float(x) for x in [open_,high,low,close]]
                    dt = datetime.datetime(int(ds[0]),int(ds[1]),int(ds[2]))
                    self.append(dt,open_,high,low,close,volume)
            elif source == 'google':
                for bar in xrange(0,len(csv)-1):
                    ds,open_,high,low,close,volume = csv[bar].rstrip().split(',')
                    open_,high,low,close,volume = [x.replace('-', '0') for x in [open_,high,low,close,volume]]
                    open_,high,low,close = [float(x) for x in [open_,high,low,close]]
                    dt = datetime.datetime.strptime(ds,'%d-%b-%y')
                    self.append(dt,open_,high,low,close,volume)
        else:
            print("The following URL for symbol '%s' did not work:\n%s" % (symbol, url_string))
            print("Consider checking the symbol or that the length of request is greater than 30 days.")


if __name__ == '__main__':
    from data_processing import load_tickers
    db_path = 'data/daily_gold.db'
    ticker_path = 'symbols/gold_picks.csv'
    #ticker_path = 'symbols/gold_gdx.csv'

    tickers, rand_state = load_tickers(validate=False, db_path=db_path, ticker_path=ticker_path, min_samples=1)

    print "Downloading Stock Prices!"
    for symbol in tickers:
        t1 = DailyQuotes(symbol     = symbol,
                         start_date = '2007-09-23',  # '2007-01-01', 
                         db_path    = db_path)

        print("%s: %s" % (symbol, len(t1.date)))

        if len(t1.date) > 1: 
            t1.overwrite_db()
            print "Updated database %s with table for symbol %s" % (db_path, symbol)







    

    #tickers = ['ABX', 'AEM', 'AG', 'AGI', 'ALIAF', 'ANVGQ', 'AU', 'AUY', 'BCEKF', 'BTG', 'CDE', 'DRGDF', 'EDVMF', 
    #           'EGO', 'FNV', 'FSM', 'GFI', 'GOLD', 'GORO', 'GG', 'HL', 'HMY', 'IAG', 'KGC', 'MAG', 'NCMGY', 'NEM', 
    #           'NGD', 'PAAS', 'PPP', 'RGLD', 'SA', 'SAND', 'SEMFF', 'SGSVF', 'SLW', 'SSRI', 'TAHO', 'TORXF', 'VNNHF',
    #           ]