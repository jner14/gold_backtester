from gbutils import *
from quote_manager import QuoteManager
import time
import datetime
import sys
import pandas as pd
import os

pd.set_option('display.width', None)

# File Paths
PICKS_CSV_PATH  = 'symbols/gold_picks.csv'
DB_FILEPATH     = 'data/daily_gold.db'
SIGNALS_PATH    = 'signals/signal_data.csv'
OUTPUT_PATH     = 'output'
MKT_CAPS        = 'data/mkt cap.csv'

# Parameters
REBAL_PERIOD    = 'M'                   # Time between rebalance, D - Daily, W - Weekly, M - Monthly, Q - Quarterly
START_DAY       = '2008_02_02'          # Day of initial stock purchases  'YYYY_MM_DD' ex '2016_01_04' '2008_01_02'
CMMSSN_SLPPG    = .00005                # Commission and slippage as a percent taken from each rebalance return
DEBUGGING_STATE = True                  # Whether or not to print debug messages to console
TDR_MA          = 10                    # The moving average length for TDR
MIN_MKT_CAP     = 100                   # Minimum market cap in millions for portfolio companies

# Create debug object
dp = Debug_Printer(DEBUGGING_STATE)

# Create QuoteManager object
quoteManager = QuoteManager(DB_FILEPATH, dp)

# Create dataframe to store return values
history = pd.DataFrame(index=["Long",
                              "GDX",
                              "GLD",
                              "Longs vs GDX",
                              "Longs vs GLD",
                              "Long vs GDX Weighted",
                              "Long vs GLD Weighted",
                              "Portfolio Additions",
                              "Portfolio Deletions"])
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
signalDates = signals.loc[signals.index >= START_DAY].index

# Load market caps, fill nan values with 0, parse dates to desired format and remove NaT rows
try:
    mktCaps = pd.read_csv(MKT_CAPS, index_col=0, parse_dates=True).fillna(0)
    mktCaps.index = mktCaps.index.strftime('%Y_%m_%d')
    if "NaT" in mktCaps.index:
        mktCaps.drop("NaT", inplace=True)
except Exception as e:
    print(e)
    print("Failed to load market caps file %s!" % MKT_CAPS)
    sys.exit()

# Get re-balance dates with a period equal to REBAL_PERIOD
rebalDates = get_calc_days(signalDates, REBAL_PERIOD)

# Get calculation days which are daily market days
calcDates = get_calc_days(signalDates, 'D')

longs = None
prevLongs = []

# Iterate through re-balance dates, calculating returns
for i in range(len(calcDates)):
    # Skip last days
    if i != len(calcDates) - 1:
        dateNow, dateNext = calcDates[i: i + 2]
    else:
        continue

    # Keep prevLongs to track changes in portfolio companies
    if longs is not None:
        prevLongs = longs.index

    # Get picks companies whose signal is positive and mkt cap is > MIN_MKT_CAP using ATR and signal rank
    # If the current date is a re-balance date then re-balance
    if calcDates[i] in rebalDates or i == 0:
        longs = get_long_positions(signals, dateNow, TDR_MA, mktCaps, MIN_MKT_CAP, quoteManager)
    else:
        longs.close = [quoteManager.get_quote(sym, dateNow) for sym in longs.index]

    nextSignalPrices = pd.Series([quoteManager.get_quote(sym, dateNext) for sym in signals.columns],
                                 index=signals.columns)

    # Get hedge quotes
    thisGDXPrice = quoteManager.get_quote('GDX', dateNow)
    thisGLDPrice = quoteManager.get_quote('GLD', dateNow)
    nextGDXPrice = quoteManager.get_quote('GDX', dateNext)
    nextGLDPrice = quoteManager.get_quote('GLD', dateNext)

    # Get portfolio vs hedge weights  1 / (longs.vol * 100)
    if calcDates[i] in rebalDates or i == 0:
        pfVola      = 1 / ((longs.atr / longs.close).mean() * 100)
        gdxVola     = 1 / ((quoteManager.get_atr('GDX', dateNow, TDR_MA) / thisGDXPrice) * 100)
        gldVola     = 1 / ((quoteManager.get_atr('GLD', dateNow, TDR_MA) / thisGLDPrice) * 100)
        longGdxWt   = pfVola / (gdxVola + pfVola)
        longGldWt   = pfVola / (gldVola + pfVola)
        gdxWt       = 1 - longGdxWt
        gldWt       = 1 - longGldWt

    # Calculate returns
    longs['nextClose'] = nextSignalPrices
    longs['return'] = nextSignalPrices / longs.close - 1
    longs['wReturn'] = longs['return'] * longs.posSize

    # Print portfolio and hedge returns
    dp.to_console("\nLong Returns for %s" % dateNext)
    dp.to_console(longs)

    # Finish calculating returns and then save values to history
    longReturn    = longs['wReturn'].sum()
    # longReturn    = (longs['return'] * (.5/len(longs))).sum()
    longReturn   -= abs(longReturn) * CMMSSN_SLPPG
    gdxReturn     = nextGDXPrice / thisGDXPrice - 1
    gdxReturn    -= abs(gdxReturn) * CMMSSN_SLPPG
    gldReturn     = nextGLDPrice / thisGLDPrice - 1
    gldReturn    -= abs(gldReturn) * CMMSSN_SLPPG
    longVsGDX     = longReturn - gdxReturn
    longVsGLD     = longReturn - gldReturn
    longVsGDXw    = longGdxWt * longReturn - gdxWt * gdxReturn
    longVsGLDw    = longGldWt * longReturn - gldWt * gdxReturn
    additions     = concat_str([x for x in longs.index if x not in prevLongs])
    deletions     = concat_str([x for x in prevLongs if x not in longs.index])

    history[dateNext] = [longReturn, gdxReturn, gldReturn, longVsGDX, longVsGLD, longVsGDXw, longVsGLDw, additions, deletions]


# Calculate Returns "GDX Weighted", "Longs vs GDX", "Long vs GDX Weighted"
longTotal               = reduce(lambda x, y: x * y, (history.loc['Long'] + 1)) - 1
gdxTotal                = reduce(lambda x, y: x * y, (history.loc['GDX'] + 1)) - 1
gldTotal                = reduce(lambda x, y: x * y, (history.loc['GLD'] + 1)) - 1
longVsGdxTotal          = reduce(lambda x, y: x * y, (history.loc['Longs vs GDX'] + 1)) - 1
longVsGldTotal          = reduce(lambda x, y: x * y, (history.loc['Longs vs GLD'] + 1)) - 1
longVsGdxWeightedTotal  = reduce(lambda x, y: x * y, (history.loc['Long vs GDX Weighted'] + 1)) - 1
longVsGldWeightedTotal  = reduce(lambda x, y: x * y, (history.loc['Long vs GLD Weighted'] + 1)) - 1

history['Totals'] = [longTotal, gdxTotal, gldTotal, longVsGdxTotal, longVsGldTotal,
                     longVsGdxWeightedTotal, longVsGldWeightedTotal, '', '']

# Save values to a csv file
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)
timestamp = str(datetime.datetime.fromtimestamp(time.time()).strftime('__%Y-%m-%d__%H-%M-%S'))
history.to_csv(OUTPUT_PATH + '/history{}.csv'.format(timestamp))

# Print Returns
dp.to_console("\n\n")
dp.to_console("Total Long Return    : {0:.2f}%".format(longTotal * 100))
dp.to_console("Total GDX Return     : {0:.2f}%".format(gdxTotal * 100))
dp.to_console("Total GLD Return     : {0:.2f}%".format(gldTotal * 100))
dp.to_console("Longs vs GDX         : {0:.2f}%".format(longVsGdxTotal * 100))
dp.to_console("Longs vs GLD         : {0:.2f}%".format(longVsGldTotal * 100))
dp.to_console("Longs vs GDX Weighted: {0:.2f}%".format(longVsGdxWeightedTotal * 100))
dp.to_console("Longs vs GLD Weighted: {0:.2f}%".format(longVsGldWeightedTotal * 100))
dp.to_console("\n\n")
print("Finished!")
