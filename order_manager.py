class OrderManager(object):
    """Handles the buying and selling of stock"""

    def __init__(self, 
                 quote_manager,
                 account,
                 slippage=.05, 
                 commission_min=1., 
                 commission=.01
                 ):
        self._quote_manager = quote_manager
        self.account = account
        self.slippage = slippage
        self.commission_min = commission_min
        self.commission = commission
        return super(OrderManager, self).__init__()


    def cover(self, amount, symbol, date):
        price = self._quote_manager.get_quote(symbol, date)
        order_qty = int(amount / price)
        return self._post_order(order_qty, symbol, price, 'cover')


    def cover_all(self, symbol, date):
        price = self._quote_manager.get_quote(symbol, date)
        order_qty = abs(self.account.get_positions().loc[symbol].qty)
        return self._post_order(order_qty, symbol, price, 'cover')


    def short(self, amount, symbol, date):
        # TODO: consider checking for counter positions
        price = self._quote_manager.get_quote(symbol, date)
        order_qty = int(amount / price)
        return self._post_order(order_qty, symbol, price, 'short')


    def sell(self, amount, symbol, date):
        price = self._quote_manager.get_quote(symbol, date)
        order_qty = int(amount / price)
        return self._post_order(order_qty, symbol, price, 'sell')


    def sell_all(self, symbol, date):
        price = self._quote_manager.get_quote(symbol, date)
        order_qty = self.account.get_positions().loc[symbol].qty
        return self._post_order(order_qty, symbol, price, 'sell')


    def buy(self, amount, symbol, date):
        # TODO: consider checking for counter positions
        price = self._quote_manager.get_quote(symbol, date)
        order_qty = int(amount / price)
        return self._post_order(order_qty, symbol, price, 'long')


    def _post_order(self, order_qty, symbol, price, order_type):

        # Determine account changes based on order type
        if order_type == 'cover':
            stock_func = self.account.remove_stock
            cash_func = self.account.deposit_cash
            multiplier = 1
        elif order_type == 'short':
            stock_func = self.account.add_stock
            cash_func = self.account.withdraw_cash
            multiplier = -1
        elif order_type == 'sell':
            stock_func = self.account.remove_stock
            cash_func = self.account.deposit_cash
            multiplier = -1
        elif order_type == 'long':
            stock_func = self.account.add_stock
            cash_func = self.account.withdraw_cash
            multiplier = 1
        else:
            raise Exception, "Invalid order_type in order_manager._post_order()!!"
        
        succeeded = False

        while not succeeded:
            commission_total = max((self.commission * order_qty), self.commission_min)
            cost = order_qty * (price + self.slippage) + commission_total
            succeeded = cash_func(cost)

            if succeeded:
                # TODO: consider adding slippage and commission to average price per share
                stock_func(symbol, order_qty * multiplier, price)
            else:
                order_qty -= 1
                if order_qty <= 0: 
                    break

        return {'qty': order_qty * multiplier, 'price': price, 'cost': cost}


# Used for debugging and development
if __name__ == '__main__':
    from account_manager import AccountManager
    am = AccountManager(100000.)
    om = OrderManager('data/daily_gold.db', am)
    pass
