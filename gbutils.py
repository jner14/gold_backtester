import numpy as np
import pandas as pd
from datetime import date as dt_date, datetime, timedelta
import sys

DEBUGGING_STATE = True                     # Whether or not to print debug messages to console


# Get top GDX component stock based on greatest market value but excluding exclude_stock 
def get_top_gdx(gdx_components, date, quote_manager, exclude_stock=None, count=10):

    # Remove any symbols that don't have quote data available for current date
    quotes = pd.Series()
    gdx_refined = gdx_components.copy()
    for symbol in gdx_refined:
        quotes[symbol] = quote_manager.get_quote(symbol, date)

    quotes.dropna(inplace=True)

    assert exclude_stock is not None, 'Error in get_top_gdx(): exclude_stock is None'
    quotes = quotes[~quotes.index.isin(exclude_stock.index)]

    top_gdx = pd.DataFrame(index=quotes.index)
    top_gdx["price"] = quotes

    if len(top_gdx) == 0:
        dp.to_console("NO VALID GDX COMPONENT STOCK FOUND FOR DATE: %s" % date)
    if count == 0:
        return top_gdx
    else:
        return top_gdx.head(count)


# Get a date based on a given date plus an offset of days
def get_date_offset(start_date = '2008_02_29', offset = -20):
    delta = timedelta(days = offset)
    year, month, day = start_date.split('_')
    start = dt_date(int(year), int(month), int(day))
    new_date = start + delta
    return new_date.strftime('%Y_%m_%d')


def get_long_positions(signals, date, tdr_ma, quote_manager):
    """
    Uses TDR as a measure volatility to weight positions with a positive signal value.
    :param signals: A DataFrame of signal values where positive values may indicate a good long position
    :param date: The date in question, necessary in order to load price quotes from the database
    :param quote_manager: A quote manager object allows price quotes to be requested from the database
    :param tdr_ma: the moving average length of tdr
    :return: A DataFrame of positions, their prices, and percent portion of portfolio
    """
    # Get positive signals for the date
    daysSignals = signals.loc[date].squeeze()
    values = pd.DataFrame()
    values["signals"] = daysSignals.loc[daysSignals > 0]
    values["prevDate"] = [quote_manager.get_prev_date(sym, date) for sym in values.index]
    values["prevClose"] = [quote_manager.get_quote(r.name, r.prevDate) for k, r in values.iterrows()]
    values["close"] = [quote_manager.get_quote(sym, date) for sym in values.index]
    # values["high"] = [quote_manager.get_quote(sym, date, "High") for sym in values.index]
    # values["low"] = [quote_manager.get_quote(sym, date, "Low") for sym in values.index]
    values["atr"] = [quote_manager.get_atr(sym, date, tdr_ma) for sym in values.index]
    values["vol"] = values.atr / values.close
    values["vol"] = values.vol / values.vol.sum()
    values["inverseVol"] = 1 / (values.vol * 100)
    # values["inverseVol"] = -(values.vol - values.vol.min() - values.vol.max())
    values["inverseVol"] = values.inverseVol / values.inverseVol.sum()
    values["ranking"] = values.signals / values.signals.sum()
    values["positionSize"] = values.vol + values.ranking
    values["positionSize"] = values.positionSize / values.positionSize.sum()

    return values


def tdr(prev_close, high, low):
    v1 = high - low
    v2 = abs(high - prev_close)
    v3 = abs(low - prev_close)
    return max(v1, v2, v3)


def get_undervalued_by_std(signals, date, quote_manager, multiplier):
    uv = get_valued(signals, date, quote_manager, len(signals), value_type="under")
    if uv is not None:
        uv = uv.loc[uv.signal < uv.signal.mean() - multiplier * uv.signal.std()]
    return uv


def get_undervalued(signals, date, quote_manager, count):
    """Get undervalued stock based on lowest signal value for a given date"""
    return get_valued(signals, date, quote_manager, count, value_type="under")


def get_overvalued(signals, date, quote_manager, count):
    """Get overvalued stock based on highest signal value for a given date"""
    return get_valued(signals, date, quote_manager, count, value_type="over")


# Get over or under valued stock based on highest or lowest signal value for a given date
def get_valued(signals, date, quote_manager, count, value_type="under"):
    """type can be 'under' or 'over'."""

    # Grab the signals for the date in question and order them 
    if value_type == "over":
        day_signals = signals.loc[date.replace('_', '-')].sort_values(ascending=False)
    elif value_type == "under":
        day_signals = signals.loc[date.replace('_', '-')].sort_values()

    day_signals_no_nans = day_signals.dropna()
    nan_signals = day_signals[day_signals.last_valid_index():].index[1:]

    # Print symbols that have NAN signal data
    dp.to_console("\nNAN signal data on %s:" % date)
    if len(nan_signals) > 0:
        for nan in nan_signals:
            dp.to_console(nan)
    else:
        dp.to_console("None")

    # Get quote data and remove any symbols that don't have quote data available for current date
    quotes = pd.Series()
    nan_quotes = []
    for symbol in day_signals_no_nans.index:
        quotes[symbol] = quote_manager.get_quote(symbol, date)
        if quotes[symbol] == 'nan':
            nan_quotes.append(symbol)
            quotes = quotes.drop(symbol)
            day_signals_no_nans = day_signals_no_nans.drop(symbol)
            
    combined = pd.DataFrame(index=day_signals_no_nans.index)
    combined["signal"] = day_signals_no_nans
    combined["price"] = quotes
         
    # TODO: Return top results if not negative or positive? Currently yes.
    if len(combined) > count:
        return combined[:count]
    elif len(combined) <= count:
        return combined
    else:
        dp.to_console("NO VALID UNDER/OVER VALUED STOCK FOUND FOR DATE: %s" % date)
        return None


# Get next rebalance day without affecting whats_left list
def get_next_rebal_day(whats_left, period):

    # This loop iterates through signal days until the close of month is reached
    #   and iterates through months until the period is reached
    for _ in range(period):

        # Iterate through days until end of month is reached
        i = 0
        while True:

            # Grab the next day of signal values
            whats_next = whats_left[i]
            year, month = whats_next[:4], whats_next[5:7]

            # Get month close values by making sure the following day is a new month or year
            if year != whats_left[i+1][:4] or month != whats_left[i+1][5:7]:
                # Then break from intra-month loop
                break

            i += 1

    return whats_next


# Find rebalance days at a frequency set by period including daily, weekly, monthly, and quarterly
def get_rebal_days(whats_left, period):

    whats_left = list(whats_left)

    # If period is not allowed, report and exit
    if period not in ['D', 'W', 'M', 'Q']:
        print("REBAL_PERIOD must be D, W, M, or Q.  Received: %s" % period)
        sys.exit()

    # If period is daily, return all dates
    if period == 'D':
        return whats_left

    rebalance_days = []
    while whats_left:

        # Iterate through days until end of period is reached
        while True:

            # Grab the current date from signal data
            whats_now = whats_left.pop(0)

            # If this is the last day of data, return
            if len(whats_left) <= 0:
                return rebalance_days

            # Grab the next date from signal data
            whats_next = whats_left[0]

            year, month, day = int(whats_now[:4]), int(whats_now[5:7]), int(whats_now[8:])
            next_year, next_month, next_day = int(whats_next[:4]), int(whats_next[5:7]), int(whats_next[8:])
            weekday = dt_date(year, month, day).isoweekday()
            next_weekday = dt_date(next_year, next_month, next_day).isoweekday()

            if period == 'W':

                # If weekday is greater than or equal to next_weekday, then break because it is the end of a week
                if weekday >= next_weekday:
                    # Then break from loop
                    break

            elif period == 'M':

                # if the current month is different from the next_month, then break because it is the end of a month
                if month != next_month:
                    # Then break from loop
                    break

            elif period == 'Q':

                # If current month is 3, 6, 9, or 12 and not equal to next_month, then break because it is the end of the quarter
                if month != next_month and month in [3, 6, 9, 12]:
                    # Then break from loop
                    break

        # Add to rebalance days
        rebalance_days.append(whats_now)

        
    print("get_rebal_days() exited without returning a value.")
    exit()
    #return rebalance_days


# A class for debug printing
class Debug_Printer(object):


    def __init__(self, debugging_state=True):
        self.debugging_state = debugging_state
        return super(Debug_Printer, self).__init__()


    def to_console(self, content):
        if self.debugging_state:
            print(content)
        else:
            print('.'),


# Get change in value
def get_return(new_value, old_value): 
    return new_value / old_value - 1


# Create debug object
dp = Debug_Printer(DEBUGGING_STATE)


# For debugging purposes
if __name__ == '__main__':
    print(get_date_offset(start_date='2008_01_05'))