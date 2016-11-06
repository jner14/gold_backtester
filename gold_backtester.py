import pandas as pd
from gbutils import *
from account_manager import AccountManager
from order_manager import OrderManager
from quote_manager import QuoteManager
import matplotlib.pyplot as plt

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
COMMISSION      = .005                      # Cost in dollars per share traded
COMMISSION_MIN  = 1.                        # Minimum cost in dollars per stock traded
COMMISSION_MAX  = .005                      # Maximum cost in percent of trade value
SLIPPAGE        = .01                       # Average slippage in price due to market volatility

#COMMISSION      = .0                        # Cost in dollars per share traded
#COMMISSION_MIN  = .0                        # Minimum cost in dollars per stock traded
#COMMISSION_MAX  = .0                        # Maximum cost in percent of trade value
#SLIPPAGE        = .0                        # Average slippage in price due to market volatility


# Create QuoteManager object
quote_manager = QuoteManager(DB_FILEPATH)

# Create AccountManager object
my_account = AccountManager(START_BALANCE, MARGIN_PERCENT, quote_manager)

# Create OrderManager object
order_manager = OrderManager(quote_manager, 
                             my_account,
                             slippage       = SLIPPAGE,
                             commission_min = COMMISSION_MIN,
                             commission     = COMMISSION,
                             commission_max = COMMISSION_MAX
                             )

# Create an order_history DataFrame
#order_history = pd.DataFrame(columns=['qty', 'price', 'cost'])
order_history = {}

# Load signals data
signals = pd.read_csv(SIGNALS_PATH, index_col=0)
signal_dates = [date.replace('-', '_') for date in signals.index]

# Load GDX component symbols
gdx_symbols = pd.read_csv(GDX_CSV_PATH).symbol
gdx_symbols = gdx_symbols[gdx_symbols.isin(signals.columns)]

# Skip days until START_DAY is found
while signal_dates[0] != START_DAY: signal_dates.pop(0)

# Create variables to store data from the backtest to be saved in output folder
index = ['Portfolio_Value', 'Cash', 'Long_Value', 'Short_Value', 'Long_Return', 'Short_Return'] + \
        ['Long_Position {}'.format(i+1) for i in range(10)] + \
        ['Short_Position {}'.format(i+1) for i in range(10)] + \
        ['Trade_{}'.format(i+1) for i in range(40)] 
history = pd.DataFrame(index=index)

# Get month close rebalance days determined by REBAL_PERIOD
rebalance_days = get_rebal_days(signal_dates, REBAL_PERIOD)

# Perform rebalancing every REBAL_PERIOD of months
old_date        = None
old_long_value  = None
for date in rebalance_days:

    print(" "*85 + date)

    # Get total and 5 percent of account value
    pre_account_value = my_account.get_account_value(date)
    cash = my_account.get_cash_value()

    # Get margin value
    margin_value = pre_account_value * MARGIN_PERCENT/100.

    # Get undervalued_stock for current date
    new_undervalued = get_undervalued(signals, date, quote_manager)

    # Get top gdx stock excluding undervalued_stock for current date
    new_top_gdx = get_top_gdx(gdx_symbols, quote_manager, new_undervalued)
    
    # Get positions for calculating unrealized returns
    long_positions = my_account.get_long_positions().index
    long_value = my_account.get_long_value(date)

    short_positions = my_account.get_short_positions().index
    short_value = my_account.get_short_value(date)

    # Get unrealized returns
    if old_long_value != None:

        # Add margin returns
        margin_gains = long_value - old_long_value + abs(short_value) - abs(old_short_value)
        #my_account.deposit_cash(margin_gains)

        # Calculate long and short returns
        long_return = get_return(long_value, old_long_value)
        #long_return *= MARGIN_PERCENT/100. + 1.
        short_return = get_return(short_value, old_short_value)
        #short_return *= MARGIN_PERCENT/100. + 1.
    else:
        long_return = 0
        short_return = 0
    
    # Sell stock no longer on undervalued list
    long_positions = my_account.get_long_positions()
    for stock in long_positions.index:
        if stock not in new_undervalued.index:
            order_history[stock] = order_manager.sell_all(stock, date)
            print('Sold %s because it is no longer on undervalued list' % stock)
            
    # Sell portion of stock on undervalued list that exceeds 5% of account value
    long_positions = my_account.get_long_positions()
    for stock in long_positions.index:
        account_value = my_account.get_account_value(date)
        five_percent_account = .05 * account_value
        current_price = quote_manager.get_quote(stock, date)
        value = abs(my_account.get_position_value(stock, date))
        diff_value = value - five_percent_account
        if diff_value > current_price:
            order_history[stock] = order_manager.sell(diff_value + current_price, stock, date)
            print('Sold some of %s because its value exceeds 5%% of portfolio' % stock)
            new_comp = 100.0 * my_account.get_position_value(stock, date) / account_value
            print('New % of portfolio for {}: {:.3}'.format(stock, new_comp))

    # Cover stock that now appears on undervalued list and that no longer is on gdx list
    short_positions = my_account.get_short_positions()
    for stock in short_positions.index:
        if stock in new_undervalued.index:
            order_history[stock] = order_manager.cover_all(stock, date, )
            print('Covered %s because it is now on undervalued list' % stock)
        elif stock not in new_top_gdx:
            order_history[stock] = order_manager.cover_all(stock, date)
            print('Covered %s because it is no longer on gdx list' % stock)
            
    #TODO: This rebalance action is not working, find out why
    # Cover portion of stock on gdx list that exceeds 5% of account value
    short_positions = my_account.get_short_positions()
    for stock in short_positions.index:
        account_value = my_account.get_account_value(date)
        five_percent_account = .05 * account_value
        current_price = quote_manager.get_quote(stock, date)
        value = abs(my_account.get_position_value(stock, date))
        diff_value = value - five_percent_account
        if diff_value > current_price:
            order_history[stock] = order_manager.cover(diff_value + current_price, stock, date)
            print('Covered some of %s because its value exceeds 5%% of portfolio' % stock)
            new_comp = 100.0 * my_account.get_position_value(stock, date) / account_value
            print('New % of portfolio for {}: {:.3}'.format(stock, new_comp))

    # Buy stock new to undervalued list
    long_positions = my_account.get_long_positions()
    for stock in new_undervalued.index:
        if stock not in long_positions.index:
            account_value = my_account.get_account_value(date)
            five_percent_account = .05 * account_value
            order_history[stock] = order_manager.buy(five_percent_account, stock, date)
            print('Bought %s because it is now on the undervalued list' % stock)

    # Buy more of stock on undervalue list that is below 5% of account value
    long_positions = my_account.get_long_positions()
    for stock in long_positions.index:
        account_value = my_account.get_account_value(date)
        five_percent_account = .05 * account_value
        current_price = quote_manager.get_quote(stock, date)
        value = abs(my_account.get_position_value(stock, date))
        diff_value = five_percent_account - value
        if diff_value > current_price:
            order_history[stock] = order_manager.buy(diff_value, stock, date)
            print('Bought some more of %s because its value falls below 5%% of portfolio' % stock)
            new_comp = 100.0 * my_account.get_position_value(stock, date) / account_value
            print('New % of portfolio for {}: {:.3}'.format(stock, new_comp))

    #TODO: This rebalance action is not working, find out why
    # Short more of stock on gdx list that is below 5% of account value
    short_positions = my_account.get_short_positions()
    for stock in short_positions.index:
        account_value = my_account.get_account_value(date)
        five_percent_account = .05 * account_value
        current_price = quote_manager.get_quote(stock, date)
        value = abs(my_account.get_position_value(stock, date))
        diff_value = five_percent_account - value
        if diff_value > current_price:
            order_history[stock] = order_manager.short(diff_value, stock, date)
            print('Shorted some more of %s because its value falls below 5%% of portfolio' % stock)
            new_comp = 100.0 * my_account.get_position_value(stock, date) / account_value
            print('New % of portfolio for {}: {:.3}'.format(stock, new_comp))

    # Short stock that no longer appears on undervalued list that is on gdx list
    short_positions = my_account.get_short_positions()
    for stock in new_top_gdx:
        account_value = my_account.get_account_value(date)
        five_percent_account = .05 * account_value
        if stock not in short_positions.index:
            order_history[stock] = order_manager.short(five_percent_account, stock, date)
            print('Shorted %s because it is now on the gdx list' % stock)
    
    # Shift variables for next rebalance
    undervalued_stock = new_undervalued
    top_gdx = new_top_gdx
    
    # Store transaction and account data from this rebalance
    long_positions = my_account.get_long_positions().index
    old_long_value = my_account.get_long_value(date)

    short_positions = my_account.get_short_positions().index
    old_short_value = my_account.get_short_value(date)

    history[date] = [pre_account_value, cash, long_value, short_value, long_return, short_return]             + \
                    [(stock, my_account.get_position_value(stock, date)) for stock in long_positions]     + \
                    [(stock, my_account.get_position_value(stock, date)) for stock in short_positions]    + \
                    [(stock, order_results) for stock, order_results in order_history.iteritems()]        + \
                    ["" for _ in range(40-len(order_history))]
    
    old_date = date
    #my_account.get_positions().qty * [quote_manager.get_quote(stock, date) for stock in my_account.get_positions().index]
    # END REBALANCE CODE

# Handle stored data by saving files and showing graphs
import time
import datetime
timestamp = str(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S'))
history.to_csv(OUTPUT_PATH + 'history_{}.csv'.format(timestamp))

# Calculate Returns
amlr = (history.loc['Long_Return'].sum() / len(history.loc['Long_Return'])) * 100.0
amsr = (history.loc['Short_Return'].sum() / len(history.loc['Short_Return'])) * 100.0
aalr = (12 * history.loc['Long_Return'].sum() / len(history.loc['Long_Return'])) * 100.0
aasr = (12 * history.loc['Short_Return'].sum() / len(history.loc['Short_Return'])) * 100.0
tlr  = history.loc['Long_Return'].sum() * 100.0
tsr  = history.loc['Short_Return'].sum() * 100.0

# Print Returns
print("\n\n")
print("Average Monthly Long Return  : {0:.2f}%".format(amlr))
print("Average Monthly Short Return : {0:.2f}%".format(amsr))
print("Average Annual Long Return   : {0:.2f}%".format(aalr))
print("Average Annual Short Return  : {0:.2f}%".format(aasr))
print("Total Long Return            : {0:.2f}%".format(tlr))
print("Total Short Return           : {0:.2f}%".format(tsr))
print("\n\n")
print("Finished!")

# Display Graphs
spy_quotes = [quote_manager.get_quote('SPY', date) for date in history.columns]
multiplier = START_BALANCE / spy_quotes[0]
spy_quotes = [multiplier * price for price in spy_quotes]
data_to_plot = pd.DataFrame()
data_to_plot['Portfolio_Value'] = history.loc['Portfolio_Value']
data_to_plot['SPY'            ] = spy_quotes
plt.figure()
data_to_plot.plot()
plt.show()
