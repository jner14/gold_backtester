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
SIGNALS_PATH    = 'csv/signal_data.csv'

# Parameters
REBAL_PERIOD    = 1                         # Number of months between rebalance
START_BALANCE   = 100000.                   # Starting cash balance in portfolio
START_DAY       = '2007_01_31'              # Day of initial stock purchases

# Create QuoteManager object
quote_manager = QuoteManager(DB_FILEPATH)

# Create AccountManager object
my_account = AccountManager(START_BALANCE, quote_manager)

# Create OrderManager object
order_manager = OrderManager(quote_manager, my_account)

# Load signals data
signals = pd.read_csv(SIGNALS_PATH, index_col=0)
whats_left = [date.replace('-', '_') for date in signals.index]
whats_done = []

# Load GDX component symbols
gdx_symbols = pd.read_csv(GDX_CSV_PATH).symbol
gdx_symbols = gdx_symbols[gdx_symbols.isin(signals.columns)]

# Skip days until START_DAY is found
while whats_left[0] != START_DAY: whats_left.pop(0)
whats_now = whats_left.pop(0)

# Get undervalued stock for initial long positions
undervalued_stock = get_undervalued(signals, whats_now, quote_manager)

# Get top GDX stock for initial short positions, excluding undervalued_stock
top_gdx_stock = get_top_gdx(gdx_symbols, quote_manager, undervalued_stock)

# Get five percent of current portfolio value
dollar_amount = my_account.get_percent_account_value(.05)

# Execute initial buy orders
for stock in undervalued_stock.index:
    confirmation = order_manager.buy(dollar_amount, stock, whats_now)
    print('Bought %s because it is now on the undervalued list' % stock)

# Execute initial short orders
for stock in top_gdx_stock: 
    confirmation = order_manager.short(dollar_amount, stock, whats_now)
    print('Shorted %s because it is now on the gdx list' % stock)

# Get month close rebalance days determined by REBAL_PERIOD
rebalance_days = get_rebal_days(whats_left, REBAL_PERIOD)

# Perform rebalancing every REBAL_PERIOD of months
history = pd.DataFrame(columns=['Account Value', 'Cash'])
for date in rebalance_days:

    print(" "*85 + date)

    # Get total and 5 percent of account value
    account_value = my_account.get_account_value(date)
    five_percent_account = .05 * account_value

    # Get undervalued_stock for current date
    new_undervalued = get_undervalued(signals, date, quote_manager)

    # Get top gdx stock excluding undervalued_stock for current date
    new_top_gdx = get_top_gdx(gdx_symbols, quote_manager, new_undervalued)
    
    # Sell stock no longer on undervalued list
    long_positions = my_account.get_long_positions()
    for stock in long_positions.index:
        if stock not in new_undervalued.index:
            confirmation = order_manager.sell_all(stock, date)
            print('Sold %s because it is no longer on undervalued list' % stock)
            
    # Sell portion of stock on undervalued list that exceeds 5% of account value
    long_positions = my_account.get_long_positions()
    for stock in long_positions.index:
        account_value = my_account.get_account_value(date)
        five_percent_account = .05 * account_value
        current_price = quote_manager.get_quote(stock, date)
        value = current_price * abs(long_positions.qty[stock])
        if value > five_percent_account:
            diff_value = value - five_percent_account
            confirmation = order_manager.sell(diff_value + current_price, stock, date)
            print('Sold some of %s because its value exceeds 5%% of portfolio' % stock)
            new_comp = 100.0 * my_account.get_value_stock(stock) / account_value
            print('New % of portfolio for {}: {:.3}'.format(stock, new_comp))

    # Cover stock that now appears on undervalued list and that no longer is on gdx list
    short_positions = my_account.get_short_positions()
    for stock in short_positions.index:
        if stock in new_undervalued.index:
            confirmation = order_manager.cover_all(stock, date, )
            print('Covered %s because it is now on undervalued list' % stock)
        elif stock not in new_top_gdx:
            confirmation = order_manager.cover_all(stock, date)
            print('Covered %s because it is no longer on gdx list' % stock)

    # Cover portion of stock on gdx list that exceeds 5% of account value
    short_positions = my_account.get_short_positions()
    for stock in short_positions.index:
        account_value = my_account.get_account_value(date)
        five_percent_account = .05 * account_value
        current_price = quote_manager.get_quote(stock, date)
        value = current_price * abs(short_positions.qty[stock])
        if value > five_percent_account:
            diff_value = value - five_percent_account
            confirmation = order_manager.cover(diff_value + current_price, stock, date)
            print('Covered some of %s because its value exceeds 5%% of portfolio' % stock)
            new_comp = 100.0 * my_account.get_value_stock(stock) / account_value
            print('New % of portfolio for {}: {:.3}'.format(stock, new_comp))

    # Buy stock new to undervalued list
    long_positions = my_account.get_long_positions()
    for stock in new_undervalued.index:
        account_value = my_account.get_account_value(date)
        five_percent_account = .05 * account_value
        if stock not in long_positions.index:
            confirmation = order_manager.buy(five_percent_account, stock, date)
            print('Bought %s because it is now on the undervalued list' % stock)

    # Buy more of stock on undervalue list that is below 5% of account value
    long_positions = my_account.get_long_positions()
    for stock in long_positions.index:
        account_value = my_account.get_account_value(date)
        five_percent_account = .05 * account_value
        current_price = quote_manager.get_quote(stock, date)
        value = current_price * abs(long_positions.qty[stock])
        if value < five_percent_account:
            diff_value = five_percent_account - value
            confirmation = order_manager.buy(diff_value, stock, date)
            print('Bought some more of %s because its value falls below 5%% of portfolio' % stock)
            new_comp = 100.0 * my_account.get_value_stock(stock) / account_value
            print('New % of portfolio for {}: {:.3}'.format(stock, new_comp))

    # Short stock that no longer appears on undervalued list that is on gdx list
    short_positions = my_account.get_short_positions()
    for stock in new_top_gdx:
        account_value = my_account.get_account_value(date)
        five_percent_account = .05 * account_value
        if stock not in short_positions.index:
            confirmation = order_manager.short(five_percent_account, stock, date)
            print('Shorted %s because it is now on the gdx list' % stock)

    # Short more of stock on gdx list that is below 5% of account value 
    short_positions = my_account.get_short_positions()
    for stock in short_positions.index:
        account_value = my_account.get_account_value(date)
        five_percent_account = .05 * account_value
        current_price = quote_manager.get_quote(stock, date)
        value = current_price * abs(short_positions.qty[stock])
        if value < five_percent_account:
            diff_value = five_percent_account - value
            confirmation = order_manager.short(diff_value, stock, date)
            print('Shorted some more of %s because its value falls below 5%% of portfolio' % stock)
            new_comp = 100.0 * my_account.get_value_stock(stock) / account_value
            print('New % of portfolio for {}: {:.3}'.format(stock, new_comp))

    undervalued_stock = new_undervalued
    top_gdx = new_top_gdx

    history.loc[date] = [account_value, my_account._cash]

history.cumsum()
plt.figure()
history.plot()
plt.show()

print "Finished!"
