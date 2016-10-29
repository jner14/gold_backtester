class OrderManager(object):
    """Handles the buying and selling of stock"""

    def __init__(self, 
                 quote_manager,
                 account,
                 slippage       = .01, 
                 commission_min = 1., 
                 commission     = .005,
                 commission_max = .005
                 ):
        self._quote_manager = quote_manager
        self.account        = account
        self.slippage       = slippage
        self.commission_min = commission_min
        self.commission     = commission
        self.commission_max = commission_max
        return super(OrderManager, self).__init__()


    def cover(self, amount, symbol, date):
        return self._post_order(symbol, date, 'cover', order_amt=amount)


    def cover_all(self, symbol, date):
        order_qty = abs(self.account.get_positions().loc[symbol].qty)
        return self._post_order(symbol, date, 'cover', order_qty=order_qty)


    def short(self, amount, symbol, date):
        return self._post_order(symbol, date, 'short', order_amt=amount)


    def sell(self, amount, symbol, date):
        return self._post_order(symbol, date, 'sell', order_amt=amount)


    def sell_all(self, symbol, date):
        order_qty = abs(self.account.get_positions().loc[symbol].qty)
        return self._post_order(symbol, date, 'sell', order_qty=order_qty)


    def buy(self, amount, symbol, date):
        return self._post_order(symbol, date, 'buy', order_amt=amount)


    def _post_order(self, symbol, date, order_type, order_qty=None, order_amt=None):

        # Determine account changes based on order type
        if order_type == 'cover':
            stock_func = self.account.remove_stock
            cash_func = self.account.deposit_cash
            fee_multiplier = -1
            multiplier = 1
        elif order_type == 'short':
            stock_func = self.account.add_stock
            cash_func = self.account.withdraw_cash
            fee_multiplier = -1
            multiplier = -1
        elif order_type == 'sell':
            stock_func = self.account.remove_stock
            cash_func = self.account.deposit_cash
            fee_multiplier = -1
            multiplier = -1
        elif order_type == 'buy':
            stock_func = self.account.add_stock
            cash_func = self.account.withdraw_cash
            fee_multiplier = 1
            multiplier = 1
        else:
            raise Exception, "Invalid order_type in order_manager._post_order()!!"

        price = self._quote_manager.get_quote(symbol, date) + self.slippage * multiplier

        if order_amt != None:
            order_qty = int(order_amt / price)
        
        succeeded = False
        while not succeeded:
            shares_value = order_qty * price
            commission_total = max((self.commission * order_qty), self.commission_min)
            if commission_total > 1.:
                commission_total = min(commission_total, self.commission_max * shares_value)
                
            transfer_amt = commission_total * fee_multiplier + shares_value
            if order_amt != None and transfer_amt > order_amt:
                order_qty -= 1
                continue
                if order_qty <= 0:
                    transfer_amt = 0
                    break

            succeeded = cash_func(transfer_amt)

            if succeeded:
                # TODO: consider adding commission to average price per share
                stock_func(symbol, order_qty * multiplier, price)
            else:
                order_qty -= 1
                if order_qty <= 0: 
                    transfer_amt = 0
                    break

        return {'shares'        : order_qty, 
                'price'         : price, 
                'transfer_amt'  : transfer_amt,
                'type'          : order_type,
                'commission'    : commission_total
                }


# Used for debugging and development
if __name__ == '__main__':
    from account_manager import AccountManager
    am = AccountManager(100000.)
    om = OrderManager('data/daily_gold.db', am)
    pass
