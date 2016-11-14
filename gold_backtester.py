import pandas as pd
from gbutils import *
from account_manager import AccountManager
from order_manager import OrderManager
from quote_manager import QuoteManager
import matplotlib.pyplot as plt
import time
import datetime

# File Paths
PICKS_CSV_PATH  = 'symbols/gold_picks.csv'
GDX_CSV_PATH    = 'symbols/gold_gdx.csv'
DB_FILEPATH     = 'data/daily_gold.db'
SIGNALS_PATH    = 'signals/signal_data.csv'
OUTPUT_PATH     = 'output/'

# Parameters
REBAL_PERIOD    = 1                         # Number of months between rebalance
START_BALANCE   = 100000.                   # Starting cash balance in portfolio
MARGIN_PERCENT  = 100.                      # The margin account size as a percent of account value
START_DAY       = '2008_01_02'              # Day of initial stock purchases  'YYYY_MM_DD' ex '2016_01_04' '2008_01_02'
LIST_SIZE       = 10                        # How many companies per list
DEBUGGING_STATE = True                     # Whether or not to print debug messages to console


# Create debug object
dp = Debug_Printer(DEBUGGING_STATE)

# Create QuoteManager object
quote_manager = QuoteManager(DB_FILEPATH, dp)

# Create dataframe to store return values
history = pd.DataFrame(index=["Top 10", "Bottom 10", "Top 10 GDX", "GDX", "Top 10 vs GDX", "Bottom 10 vs GDX"])

# Load signals data
signals = pd.read_csv(SIGNALS_PATH, index_col=0)
signal_dates = [date.replace('-', '_') for date in signals.index]

# Load GDX component symbols
gdx_symbols = pd.read_csv(GDX_CSV_PATH).symbol
gdx_symbols = gdx_symbols[gdx_symbols.isin(signals.columns)]

# Skip days until START_DAY is found
while signal_dates[0] != START_DAY: signal_dates.pop(0)

# Get month close rebalance days determined by REBAL_PERIOD
rebalance_days = get_rebal_days(signal_dates, REBAL_PERIOD)


### Iterate through rebalance dates calculating returns
old_date = None
for date in rebalance_days:

    # Get top 10 undervalued stock for current date
    overvalued = get_overvalued(signals, date, quote_manager, LIST_SIZE)

    # Get bottom 10 undervalued stock for current date
    undervalued = get_undervalued(signals, date, quote_manager, LIST_SIZE)

    # Get Top 10 GDX not included in undervalued list
    top_gdx = get_top_gdx(gdx_symbols, date, quote_manager, undervalued)
    
    # If this is the first date initialize variables and skip rest of loop to next date
    if old_date is None:
        old_date        = date
        old_overvalued  = overvalued
        old_undervalued = undervalued
        old_top_gdx     = top_gdx
        continue

    all_prices = pd.Series([quote_manager.get_quote(sym, date) for sym in signals.columns], index=signals.columns)

    old_overvalued['new price'] = all_prices
    old_undervalued['new price'] = all_prices
    old_top_gdx['new price'] = all_prices
    
    # Calculate returns, current/old - 1
    old_overvalued['return'] = (all_prices/old_overvalued.price - 1).dropna()
    old_undervalued['return'] = (all_prices/old_undervalued.price - 1).dropna()
    old_top_gdx['return'] = (all_prices/old_top_gdx.price - 1).dropna()

    # Print under/over valued lists
    dp.to_console("\nOvervalued Returns for %s" % date)
    dp.to_console(old_overvalued)
    dp.to_console("\nUndervalued Returns for %s" % date)
    dp.to_console(old_undervalued)
    dp.to_console("\nTop 10 GDX Returns for %s" % date)
    dp.to_console(old_top_gdx)

    # Save values to history
    Top10         = old_overvalued['return'].sum() / len(old_overvalued)
    Bottom10      = old_undervalued['return'].sum() / len(old_undervalued)
    Top10GDX      = old_top_gdx['return'].sum() / len(old_top_gdx)
    GDX           = quote_manager.get_quote('GDX', date) / quote_manager.get_quote('GDX', old_date) - 1
    Top10vsGDX    = Top10 - GDX
    Bottom10vsGDX = Bottom10 - GDX

    history[date] = [Top10, Bottom10, Top10GDX, GDX, Top10vsGDX, Bottom10vsGDX]

    # Set new as old for next rebalance
    old_date = date
    old_overvalued = overvalued
    old_undervalued = undervalued
    old_top_gdx = top_gdx


# Calculate Returns
Top10_total         = reduce(lambda x, y: x * y, (history.loc['Top 10'          ] + 1)) - 1
Bottom10_total      = reduce(lambda x, y: x * y, (history.loc['Bottom 10'       ] + 1)) - 1
Top10GDX_total      = reduce(lambda x, y: x * y, (history.loc['Top 10 GDX'      ] + 1)) - 1
GDX_total           = reduce(lambda x, y: x * y, (history.loc['GDX'             ] + 1)) - 1
Top10vsGDX_total    = reduce(lambda x, y: x * y, (history.loc['Top 10 vs GDX'   ] + 1)) - 1
Bottom10vsGDX_total = reduce(lambda x, y: x * y, (history.loc['Bottom 10 vs GDX'] + 1)) - 1

history['Totals'] = [Top10_total, Bottom10_total, Top10GDX_total, GDX_total, Top10vsGDX_total, Bottom10vsGDX_total]

# Save values to a csv file
timestamp = str(datetime.datetime.fromtimestamp(time.time()).strftime('__%Y-%m-%d__%H-%M-%S__'))
history.to_csv(OUTPUT_PATH + 'history{}.csv'.format(timestamp))

# Print Returns
dp.to_console("\n\n")
dp.to_console("Total Top 10 Return    : {0:.2f}%".format(Top10_total*100        ))
dp.to_console("Total Bottom 10 Return : {0:.2f}%".format(Bottom10_total*100     ))
dp.to_console("Total Top 10 GDX Return: {0:.2f}%".format(Top10GDX_total*100     ))
dp.to_console("Total GDX Return       : {0:.2f}%".format(GDX_total*100          ))
dp.to_console("Top 10 vs GDX          : {0:.2f}%".format(Top10vsGDX_total*100   ))
dp.to_console("Bottom 10 vs GDX       : {0:.2f}%".format(Bottom10vsGDX_total*100))
dp.to_console("\n\n")
print("Finished!")
