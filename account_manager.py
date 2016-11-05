from pandas import DataFrame, Series
from gbutils import *


class AccountManager(object):
    """Used to manage a portfolio of stock and cash."""


    def __init__(self, start_cap, margin_percent, quote_manager):
        self._cash = start_cap
        self._margin_percent = margin_percent
        self._quote_manager = quote_manager
        self._stock = DataFrame(columns=['qty', 'price'])
        return super(AccountManager, self).__init__()


    def get_margin_value(self, date):
        return self.get_account_value() * self._margin_percent/100
    # what if on covers and sells we multiply the gains by the margin percent
    # what if we keep track of amounts borrowed per stock and pay the borrow amount down before returning rest to account


    def get_margin_percent(self):
        return self._margin_percent


    def get_cash_value(self):
        return self._cash


    def get_account_value(self, date):
        # For each stock owned sum up the price at date * qty
        stock_value = 0
        for symbol in self._stock.index:
            qty = abs(self._stock.loc[symbol].qty)
            stock_value += self._quote_manager.get_quote(symbol, date) * qty

        # Return the cash in account plus the sum of stock values 
        return self._cash + stock_value


    def get_percent_account_value(self, date, percent=.05):
        return self.get_account_value(date) * percent


    def get_position_value(self, symbol, date):
        '''Returns the total value of a specified stock.'''
        return self._quote_manager.get_quote(symbol, date) * abs(self._stock.qty[symbol])


    def get_positions(self):
        '''Returns a DataFrame copy of all positions held.'''
        return self._stock.copy()


    def get_short_positions(self):
        '''Returns a DataFrame copy of short positions held.'''
        return self._stock[self._stock.qty < 0].copy()


    def get_short_value(self, date):
        '''Returns a sum of all short positions.'''
        shorts = self._stock[self._stock.qty < 0].copy()
        shorts['original_value'   ] = shorts.price * shorts.qty
        shorts['current_price'    ] = [self._quote_manager.get_quote(stock, date) for stock in shorts.index]
        shorts['current_price_qty'] = shorts.current_price * shorts.qty
        shorts['returns'          ] = ((shorts.current_price_qty / shorts.original_value) - 1) * -1
        shorts['value'            ] = (1 + shorts.returns) * shorts.original_value
        return shorts.value.sum()


    def get_long_positions(self):
        '''Returns a DataFrame copy of long positions held.'''
        return self._stock[self._stock.qty > 0].copy()


    def get_long_value(self, date):
        '''Returns a sum of all long positions.'''
        value = 0
        for stock in self._stock[self._stock.qty > 0].index:
            value += self._quote_manager.get_quote(stock, date) * self._stock.qty[stock]
        return value    


    def add_stock(self, symbol, qty, price):
        if symbol in self._stock.index:
            qty_owned = self._stock.loc[symbol].qty
            price_owned = self._stock.loc[symbol].price
            new_price = (qty_owned * price_owned + qty * price) / (qty_owned + qty)
        else:
            qty_owned = 0
            new_price = price
        self._stock.loc[symbol] = [qty_owned + qty, new_price]


    def remove_stock(self, symbol, qty, price):
        # Assert we own the stock to be removed
        assert symbol in self._stock.index, \
            'ERROR in AccountManager.remove_stock() >>' + \
            'Symbol: %s is not in account stock: %s' % (symbol, self._stock.index)
        
        # Get quantity of stock owned
        qty_owned = self._stock.loc[symbol].qty
        
        # Assert we are only trying to cover short positions or sell long positions
        assert qty_owned * qty < 0, \
            'ERROR in AccountManager.remove_stock() >>' + \
            'qty_owned (%s) and qty to be removed (%s) have identical signs.  ' % (qty_owned, qty) + \
            'Make sure you are not trying to cover a long or sell a short position.'

        # Assert we are not trying to remove more stock than we own
        assert abs(qty_owned) >= abs(qty), \
            'ERROR in AccountManager.remove_stock() >>' + \
            'qty_owned (%s) is less than qty being removed: %s' % (qty_owned, qty)

        # Adjust stock quantity by passed quantity
        self._stock.loc[symbol].qty = qty_owned + qty

        # If the quantity owned is equal to that being removed, set price == 0
        if abs(qty_owned) == abs(qty):
            self._stock.loc[symbol].price = 0.


    def deposit_cash(self, amount):
        self._cash += amount
        return True


    def withdraw_cash(self, amount):
        if self._cash >= amount:
            self._cash -= amount
            return True
        else:
            return False


# Used for debugging and development
if __name__ == '__main__':
    from quote_manager import QuoteManager
    qm = QuoteManager('data/daily_gold.db')
    am = AccountManager(100000., qm)

    am.add_stock('EDV', -20, 100.)
    print(am.get_account_value('2016_10_17'))

    am.add_stock('ABX', 20, 100.)
    print(am.get_account_value('2016_10_17'))

    am.remove_stock('ABX', -10)
    print(am.get_account_value('2016_10_17'))

    print(am._stock.loc['ABX'])

    am.add_stock('ABX', 10, 50.)
    print(am.get_account_value('2016_10_17'))

    print(am.get_position_value('ABX', '2016_10_17'))

    print(am.get_percent_account_value('2016_10_17'))

    print(am.get_long_positions())

    print(am.get_short_positions())

    pass