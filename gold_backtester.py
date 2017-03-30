import pandas as pd
from gbutils import *
from quote_manager import QuoteManager
import matplotlib.pyplot as plt
import time
import datetime
import sys

# File Paths
PICKS_CSV_PATH  = 'symbols/gold_picks.csv'
GDX_CSV_PATH    = 'symbols/gold_gdx.csv'
DB_FILEPATH     = 'data/daily_gold.db'
SIGNALS_PATH    = 'signals/signal_data.csv'
OUTPUT_PATH     = 'output/'
MKT_CAPS        = 'data/mkt cap.csv'

# Parameters
REBAL_PERIOD    = 'W'                   # Time between rebalance, D - Daily, W - Weekly, M - Monthly, Q - Quarterly
START_DAY       = '2008_01_02'          # Day of initial stock purchases  'YYYY_MM_DD' ex '2016_01_04' '2008_01_02'
CMMSSN_SLPPG    = .00005                # Commission and slippage as a percent taken from each rebalance return
LIST_SIZE       = 10                    # How many companies per list
DEBUGGING_STATE = True                  # Whether or not to print debug messages to console
TDR_MA          = 10                    # The moving average length for TDR
MIN_MKT_CAP     = 100                   # Minimum market cap in millions for portfolio companies
HEDGE_CNT       = 10                    # Number of top companies, based on mkt cap, to include in hedge portfolio


# Create debug object
dp = Debug_Printer(DEBUGGING_STATE)

# Create QuoteManager object
quote_manager = QuoteManager(DB_FILEPATH, dp)

# Create dataframe to store return values
history = pd.DataFrame(index=["Longs", "GDX", "Longs vs GDX"])

# Load signals data
try:
    signals = pd.read_csv(SIGNALS_PATH, index_col=0, parse_dates=True)
except Exception as e:
    print(e)
    print("Failed to load signals file %s!" % SIGNALS_PATH)
    sys.exit()

# Fill nan values with 0
signals = signals.fillna(0)

# Return date index to string and drop invalid rows
signals.index = signals.index.strftime('%Y_%m_%d')
if "NaT" in signals.index:
    signals.drop("NaT", inplace=True)

# Get signal dates from index
signal_dates = signals.loc[signals.index >= START_DAY].index

# Load market caps, fill nan values with 0, parse dates to desired format and remove NaT rows
try:
    mkt_caps = pd.read_csv(MKT_CAPS, index_col=0, parse_dates=True).fillna(0)
    mkt_caps.index = mkt_caps.index.strftime('%Y_%m_%d')
    if "NaT" in mkt_caps.index:
        mkt_caps.drop("NaT", inplace=True)
except Exception as e:
    print(e)
    print("Failed to load market caps file %s!" % MKT_CAPS)
    sys.exit()

# Load GDX component symbols
try:
    gdx_symbols = pd.read_csv(GDX_CSV_PATH).symbol
    gdx_symbols = gdx_symbols[gdx_symbols.isin(signals.columns)]
except Exception as e:
    print(e)
    print("Failed to load gdx symbols file %s!" % GDX_CSV_PATH)
    sys.exit()

# Get month close rebalance days determined by REBAL_PERIOD
rebalance_days = get_rebal_days(signal_dates, REBAL_PERIOD)


### Iterate through rebalance dates, calculating returns
old_date = None
old_top_gdx = None
old_longs = None
for date in rebalance_days:

    # Get most stock for current date based on standard deviation of signal values
    longs = get_long_positions(signals, date, TDR_MA, quote_manager)

    # Filter long positions by mkt cap greater than given parameter
    longs['mkt_cap'] = mkt_caps.loc[date].squeeze()
    longs = longs.loc[longs.mkt_cap > MIN_MKT_CAP]

    # Get All GDX not included in undervalued list then keep those with highest mkt_cap
    top_gdx = get_top_gdx(gdx_symbols, date, quote_manager, exclude_stock=longs, count=0)
    top_gdx['mkt_cap'] = mkt_caps.loc[date].squeeze()
    top_gdx = top_gdx.loc[top_gdx.mkt_cap > MIN_MKT_CAP].sort_values('mkt_cap', ascending=False)

    # If this is the first date initialize variables and skip rest of loop to next date
    if old_date is None:
        old_date        = date
        old_longs       = longs
        old_top_gdx     = top_gdx
        continue

    all_prices = pd.Series([quote_manager.get_quote(sym, date) for sym in signals.columns], index=signals.columns)

    longs['new price'] = all_prices
    old_top_gdx['new price'] = all_prices

    # Calculate returns, current/old - 1
    longs['return'] = (all_prices/longs.prevClose - 1).dropna()
    old_top_gdx['return'] = (all_prices/old_top_gdx.price - 1).dropna()

    # Print under/over valued lists
    dp.to_console("\nLong Returns for %s" % date)
    dp.to_console(old_longs)
    dp.to_console("\nTop 10 GDX Returns for %s" % date)
    dp.to_console(old_top_gdx)

    # Save values to history
    meanLong      = longs['return'].mean()
    meanLong     -= abs(meanLong) * CMMSSN_SLPPG
    GDX           = quote_manager.get_quote('GDX', date) / quote_manager.get_quote('GDX', old_date) - 1
    LongsvsGDX    = meanLong - GDX

    history[date] = [meanLong, GDX, LongsvsGDX]

    # Set new as old for next rebalance
    old_date = date
    old_longs = longs
    old_top_gdx = top_gdx


# Calculate Returns
Longs_Total         = reduce(lambda x, y: x * y, (history.loc['Longs'] + 1)) - 1
GDX_total           = reduce(lambda x, y: x * y, (history.loc['GDX'] + 1)) - 1
LongsvsGDX_total    = Longs_Total - GDX_total

history['Totals'] = [Longs_Total, GDX_total, LongsvsGDX_total]

# Save values to a csv file
timestamp = str(datetime.datetime.fromtimestamp(time.time()).strftime('__%Y-%m-%d__%H-%M-%S__'))
history.to_csv(OUTPUT_PATH + 'history{}.csv'.format(timestamp))

# Print Returns
dp.to_console("\n\n")
dp.to_console("Total Longs Return : {0:.2f}%".format(Longs_Total * 100))
dp.to_console("Total GDX Return   : {0:.2f}%".format(GDX_total*100))
dp.to_console("Longs vs GDX       : {0:.2f}%".format(LongsvsGDX_total * 100))
dp.to_console("\n\n")
print("Finished!")
