import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta

DEBUGGING_STATE = True                     # Whether or not to print debug messages to console


# Get top GDX component stock based on greatest market value but excluding exclude_stock 
def get_top_gdx(gdx_components, date, quote_manager, exclude_stock=None, count=10):

    # Remove any symbols that don't have quote data available for current date
    quotes = pd.Series()
    gdx_refined = gdx_components.copy()
    for symbol in gdx_refined:
        quotes[symbol] = quote_manager.get_quote(symbol, date)

    quotes.dropna()

    assert exclude_stock is not None, 'Error in get_top_gdx(): exclude_stock is None'
    quotes = quotes[~quotes.index.isin(exclude_stock.index)]

    top_gdx = pd.DataFrame(index=quotes.index)
    top_gdx["price"] = quotes

    if len(top_gdx) > count:
        return top_gdx[:count]
    else:
        if len(top_gdx) == 0: 
            dp.to_console("NO VALID GDX COMPONENT STOCK FOUND FOR DATE: %s" % date)
        return top_gdx


# Get a date based on a given date plus an offset of days
def get_date_offset(start_date = '2008_02_29', offset = -20):
    delta = timedelta(days = offset)
    year, month, day = start_date.split('_')
    start = date(int(year), int(month), int(day))
    return start + delta 


# Get undervalued stock based on lowest signal value for a given date
def get_undervalued(signals, date, quote_manager, count):
    return get_valued(signals, date, quote_manager, count, type="under")


# Get overvalued stock based on highest signal value for a given date
def get_overvalued(signals, date, quote_manager, count):
    return get_valued(signals, date, quote_manager, count, type="over")


# Get over or under valued stock based on highest or lowest signal value for a given date
def get_valued(signals, date, quote_manager, count, type="under"):
    """type can be 'under' or 'over'."""
    
    # Grab the signals for the date in question and order them 
    if type == "over":
        day_signals = signals.loc[date.replace('_', '-')].sort_values(ascending=False)
    elif type == "under":
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
    else:
        if len(combined) == 0: 
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

    # If period is not allowed, report and exit
    if period not in ['D', 'W', 'M', 'Q']:
        print("REBAL_PERIOD must be D, W, M, or Q.  Received: %s" % period)
        exit()

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
            weekday = date(year, month, day).isoweekday()
            next_weekday = date(next_year, next_month, next_day).isoweekday()

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