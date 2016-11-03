# -*- coding: utf-8 -*-
import sqlite3 as lite
import csv
import numpy as np
import random
#import cPickle
#import os
#from datetime import datetime, timedelta
from pytz import timezone

PICKS_CSV_PATH = 'symbols/gold_picks.csv'
GDX_CSV_PATH = 'symbols/gold_gdx.csv'
DB_FILEPATH = 'data/daily_gold.db'
MIN_STOCK_PRICE_HISTORY = 1
f = "{:.2f}"
time_zone = timezone('US/Eastern')
time_fmt = '%Y-%m-%d %H:%M:%S %Z%z'

def load_tickers( randomize     = False,                    # Should the selction and order be random
                  ticker_count  = 1,                        # Vaules > 1 are literal, and <= 1 are percent
                  rand_state    = 0,                        # Can feed a random state value like a seed
                  validate      = True,                     # Whether or not to validate csv values against db
                  db_path       = DB_FILEPATH,              # Filepath of db
                  ticker_path   = PICKS_CSV_PATH,          # Filepath of csv symbols
                  min_samples   = MIN_STOCK_PRICE_HISTORY   # Min number of rows in db table for the symbol to be included
                  ):
    '''Load list of ticker labels from a csv into a list of strings.
    Verifies tables with hard-coded database for length and existance.
    Optional parameter RANDOMIZE will return list randomized.
    Optional parameter TICKER_COUNT will allow a subset of tickers to
    be loaded.  Values > 1 will be considered literal, and <= 1 will be 
    considered percentage of total tickers in file read.
    Returns string list of ticker labels and a random number state'''
    #if rand_state is not an integer, assume it is a 
    if type(rand_state) != int : random.setstate(rand_state)
    else: rand_state = random.getstate()
    
    with open(ticker_path, 'r') as csvfile:
        lines = csv.reader(csvfile)
        tickers = list(lines)
        tickers.pop(0)#remove header row
        tickers = np.asarray(tickers).transpose()
        tickers = list(tickers[0])
        
        # Remove tickers if their history is too short or they don't exist in db
        if validate:
            con = lite.connect(db_path)
            for t in tickers:
                try:
                    with con:
                        cur = con.cursor()
                        cur.execute('SELECT COUNT(*) FROM [%s]' % t)
                        count = cur.fetchone()[0]
                        if count < min_samples:
                            print ('Table too short: %s' % t)
                            tickers.remove(t)
                except:
                    #print ('Table not found: %s' % t)
                    tickers.remove(t)

        #if ticker_count is passed as a < 1 value then convert to percent of all tickers
        if ticker_count <= 1 and ticker_count > 0: ticker_count = int(ticker_count * len(tickers)+0.5)

        #if randomize then take a random sample ticker_count long  
        if randomize: tickers2 = random.sample(tickers,ticker_count)

        #else take a non-random sample starting from begining of list
        else: tickers2 = tickers[:ticker_count]
        
        return tickers2, rand_state

## load_tickers() test code, leave commented out
#test_tickers, rand_state = load_tickers(ticker_path=GDX_CSV_PATH, ticker_count=1)
#print test_tickers

        
def check_missing_dates(db_path,ticker,print_res=True):
    t1 = data_capture.Ticker()
    t1.read_all(db_path,ticker)
    missing = []
    missing.append(ticker)
    missing_int = 0
    deleted = 0
    print('%s...'% ticker),
    i = 0
    for i in range(len(t1.date)-1):
        i -= deleted #adjusts index to account for deleted duplicate quotes
        if t1.date[i].isoweekday() == 5: # 5 is friday
            time_delta_int = 3
        else:
            time_delta_int = 1
        date1 = t1.date[i] + timedelta(days=time_delta_int)
        year,month,day,dow = date1.year, date1.month, date1.day, date1.isoweekday()
        if date1 != t1.date[i+1]:
            #check if it's a duplicate entry
            if t1.date[i] == t1.date[i+1]:
                print('\nFOUND DUPLICATE ENTRY!!')
                print(t1.get_quote_by_index(i))
                print(t1.get_quote_by_index(i+1))
                while True:
                    answer = raw_input("Delete first(1), second(2), or neither(3)? ")
                    if answer == '1':
                        t1.delete_quote_by_index(i)
                        deleted += 1
                        break
                    elif answer == '2':
                        t1.delete_quote_by_index(i+1)
                        deleted += 1
                        break
                    elif answer == '3':
                        break
                    else:
                        print('Your response [%s] was not an option. Try again!'% answer)
                continue
            #unless its thanksgiving
            elif month == 11 and day > 21 and day < 29 and dow == 4:
                continue
            #unless its martin luther king day
            elif month == 1 and day > 14 and day < 22 and dow == 1:
                continue
            #unless its labor day
            elif month == 9 and day > 0 and day < 8 and dow == 1:
                continue
            #unless its president's day
            elif month == 2 and day > 14 and day < 22 and dow == 1:
                continue
            #unless its memorial day
            elif month == 5 and day > 24 and day < 32 and dow == 1:
                continue
            #unless its 9-11
            elif year == 2001 and month == 9 and day == 11:
                continue
            #unless its the 4th of july
            elif month == 7 and day > 2 and day < 6:
                continue
            #unless its new years
            elif month == 1 and day > 0 and day < 3:
                continue
            #unless its christmas
            elif month == 12 and day > 23 and day < 27:
                continue
            else:
                missing_int +=1
                missing.append((year, month, day, dow))
                if print_res: print(i, year, month, day, dow)
    if print_res: print("Total Missing %s" % missing_int)
    return missing
#missing = check_missing_dates('data/daily_gold.db','A')
    
def check_erratic_values(db_path,ticker,print_res=True,erratic_index=10):
    '''Checks for missing values and drastic changes from one quote to the next'''
    t1 = data_capture.Ticker()
    t1.read_all(db_path,ticker)
    global f
    erratics = []
    erratics.append(ticker)
    erratic_cnt = 0
    for i in range(len(t1.date)-1):
#        o,c,h,l,v = t1.open_[i], t1.close[i], t1.high[i], t1.low[i], t1.volume[i]
        q1 = t1.get_quote_by_index(i)
        q2 = t1.get_quote_by_index(i+1)
        changes = []
        adj = 0
        erratic_bool = False
        for j in range(1,len(q1)):
            if q1[j] == 0 or q2[j] == 0:
                erratic_bool= True
                adj = 0.001#prevent div by 0
            chng = float(abs(q2[j] - q1[j]))/(q1[j]+adj) * 100
            chng = f.format(chng)
            changes.append(chng)
        for change in changes[0:-1]:
            if float(change) > erratic_index: 
                erratic_bool = True
        if erratic_bool:
            erratic_cnt += 1
            erratics.append((q1,q2,changes))
            if print_res: print(erratics[-1])
            
    if print_res: print("Total Erratics %s" % erratic_cnt)
    return erratics
#erratics = check_erratic_values('data/daily_gold.db','A',erratic_index=40)
    
def check_missing_dates_from_list(db_name='daily'):
    missing = []
    tickers,rand_state = load_tickers()
    db_path = 'data/%s.db'% db_name
    print('\nChecking for missing dates from %s...'% db_path)
    for ticker in tickers:
        missing.append(check_missing_dates(db_path,ticker,print_res=False))
    return missing
#missing1 = check_missing_dates_from_list('daily')
#missing2 = check_missing_dates_from_list('minutes_1')
    
def convert_csv_to_db(time_cat, db_name,intraday=True):
    empties = []
    additions = 0
    
    with open('symbols/spy500.csv', 'r') as csvfile:
        lines = csv.reader(csvfile)
        tickers = list(lines)
        tickers.pop(0)#remove header row
        tickers = np.asarray(tickers).transpose()
        print("Begining %s..." % (time_cat))
    
    for ticker in tickers[0]:
        t1 = data_capture.Ticker()
        spaces = abs(len(ticker)-5) * ' '
        tickerPrint = ticker + spaces
            
        print("-\n%s>> " % tickerPrint),
        
        print("reading csv... "),
        path = ("data/%s/%s.csv" % (time_cat, ticker))
        if os.path.exists(path):
            t1.read_csv(path)
            print("writing to db..."),
            t1.overwrite_sqlite(db_name,ticker,intraday)
            additions +=1

            print("%s... " % additions),
        else:
            empties.append(ticker)
            print("no csv found..."),
        print("COMPLETE!!")
    print("These were not converted: %s" % empties)
#convert_csv_to_db('daily','data/daily_gold.db',False)
#convert_csv_to_db('minutes_1','data/minutes_1.db',True)

def getRandoms(testSetLength, randomizeTraining):
    randoms = []
    if not randomizeTraining:
        for i in range(testSetLength):
            randoms.append(i)
    else:
        i = 0
        randomset = set()
        while len(randoms) < testSetLength:
            i += 1
            if i % 100000 == 0: 
                print('i: ' + repr(i) + ' len(randoms): ' + repr(len(randoms)))
                print(testSetLength)
            random_number = random.randrange(0,testSetLength)
            if not random_number in randomset:
                randomset.add(random_number)
                randoms.append(random_number)
    return randoms
    
def load_dataset_csv(filename):
#Takes a filename and loads it into memory.
    
    with open(filename, 'rb') as csvfile:
        lines = csv.reader(csvfile)
        dataset = list(lines)
#        for i in range(len(dataset)):
#            randoms.append(random.random())
    return dataset
    
def load_dataset_pkl(pkl_path, randomize=True, ticker_count=10):
    '''Loads a pickel dataset'''
    tickers,rand_state = load_tickers(randomize,ticker_count)
    dataset_full = load_obj(pkl_path)
    dataset = []
    print 'Loading dataset. Please wait...'
    for ticker in tickers:
        print ('%s' % ticker),
#        print ('.'),
        if len(dataset_full[ticker]) > MIN_STOCK_PRICE_HISTORY:
            dataset.append(dataset_full[ticker])
    print'\n'        
    return dataset

def save_obj(obj, name):
    try:
        with open(name + '.pkl', 'wb') as f:
            cPickle.dump(obj, f) #, cPickle.HIGHEST_PROTOCOL)
    except IOError:
        print 'COULD NOT SAVE ' + name + ' TO FILE!!'

def create_dataset_from_db(db_path,dataset_path):
    tickers,rand_state = load_tickers(randomize=False,ticker_count=1)
    dataset = {}
    print 'Loading dataset. Please wait...'
    for ticker in tickers:
        print ('%s' % ticker),
#        print ('.'),
        t1 = data_capture.Ticker()
        t1.read_db(db_path,ticker)
#        if len(t1.close) > MIN_STOCK_PRICE_HISTORY:
        dataset[ticker] = tuple(t1.close)
    save_obj(dataset, dataset_path)
    return dataset
            
def printTime():
    date_time = tyme()
    return date_time.strftime(time_fmt)
def tyme():
    """Return the date-time object with the current time. Options include:\
    .year, .month, .day, .hour, .minute, .second, .microsecond, and .tzinfo"""
    return datetime.now(time_zone)
   
        
def load_obj(name):
    try:
        with open(name + '.pkl', 'r') as f:
            return cPickle.load(f)
    except IOError:
        print 'COULD NOT OPEN ' + name + ' FILE!!'
#temp = create_dataset_from_db(DAILY_DB_PATH, DATASET_CLOSES_PATH)
#temp2 = load_obj(DATASET_CLOSES_PATH)


if __name__ == '__main__':
    db_path = 'data/daily_gold.db'
    ticker_path1 = 'symbols/gold_picks.csv'
    ticker_path2 = 'symbols/gold_gdx.csv'

    picks, rand_state = load_tickers(validate=False, db_path=db_path, ticker_path=ticker_path1, min_samples=1)
    gdx, rand_state = load_tickers(validate=False, db_path=db_path, ticker_path=ticker_path2, min_samples=1)
