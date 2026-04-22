from prosperity4bt.datamodel import TradingState, Order, Symbol, Trade
from prosperity4bt.models.input import BacktestData, MarketTrade
from prosperity4bt.models.output import TradeRow
from prosperity4bt.models.test_options import TradeMatchingMode


# Match orders that're returned from the trading algorithm to prices in BacktestData
# If orders are not fulfilled, and the trade_matching_mode is not TradeMatchingMode.none,
#  then the algorithm will try to fill the orders from the market_trades
class OrderMatchMaker:

    def __init__(self, state: TradingState, back_data: BacktestData, orders: dict[Symbol, list[Order]], trade_matching_mode: TradeMatchingMode, maf_factor: float = 1.0):
        self.state = state
        self.back_data = back_data
        self.orders = orders
        self.trade_matching_mode = trade_matching_mode
        self.maf_factor = maf_factor

    def match(self) -> list[TradeRow]:
        result = []
        market_trades = self.back_data.get_market_trades_at(self.state.timestamp)

        # Scale market trade quantities to simulate MAF effect on passive fills.
        # maf_factor=1.0 (default) = full market = winning MAF (historical data as-is).
        # maf_factor=0.8 = only 80% of fill bots are active = losing MAF.
        if self.maf_factor != 1.0:
            for product_trades in market_trades.values():
                for mt in product_trades:
                    mt.buy_quantity  = int(round(mt.buy_quantity  * self.maf_factor))
                    mt.sell_quantity = int(round(mt.sell_quantity * self.maf_factor))

        timestamp = self.state.timestamp

        for product in self.back_data.products:
            new_trades = []
            orders = self.orders.get(product, [])
            for order in orders:
                if order.quantity > 0:
                    new_trade = self.__match_buy_order_from_price_depth(order)
                else:
                    new_trade = self.__match_sell_order_from_price_depth(order)
                if len(new_trade) > 0:
                    new_trades.extend(new_trade)

            buy_order = [o for o in orders if o.quantity > 0]
            for order in sorted(buy_order, key=lambda o: o.price, reverse=True):
                new_trade = self.__match_buy_order_from_market_trades(order, market_trades.get(product, []))
                if len(new_trade) > 0:
                    new_trades.extend(new_trade)
                    break

            sell_order = [o for o in orders if o.quantity < 0]
            for order in sorted(sell_order, key=lambda o: o.price):
                new_trade = self.__match_sell_order_from_market_trades(order, market_trades.get(product, []))
                if len(new_trade) > 0:
                    new_trades.extend(new_trade)
                    break

            if len(new_trades) > 0:
                self.state.own_trades[product] = new_trades
                result.extend([TradeRow(trade) for trade in new_trades])

        # adjust market trades as market trades might have been used to fill the orders
        for product, trades in market_trades.items():
            trades_updated = False
            for trade in trades:
                if trade.buy_quantity != trade.sell_quantity:
                    trades_updated = True
                    trade.trade.quantity = min(trade.buy_quantity, trade.sell_quantity)

            # remaining_market_trades = [t.trade for t in trades if t.trade.quantity > 0]
            remaining_market_trades = [t.trade for t in trades if t.trade.quantity > 0 and t.buy_quantity == t.sell_quantity]
            self.state.market_trades[product] = remaining_market_trades
            # TODO: why adding remaining_market_trades into result?
            result.extend([TradeRow(trade) for trade in remaining_market_trades])

        return result


    def __create_buy_order(self, order: Order, volume: int, price: int, seller: str):
        self.state.position[order.symbol] = self.state.position.get(order.symbol, 0) + volume
        self.back_data.profit_loss[order.symbol] -= price * volume
        order.quantity -= volume
        return Trade(order.symbol, price, volume, "SUBMISSION", seller, self.state.timestamp)

    def __can_match_buy_order(self, order: Order, market_trade: MarketTrade) -> bool:
        if market_trade.sell_quantity == 0:
            return False
        if market_trade.trade.price > order.price:
            return False
        if market_trade.trade.price == order.price:
            return self.trade_matching_mode == TradeMatchingMode.all
        return True

    def __create_sell_order(self, order: Order, volume: int, price: int, buyer: str):
        self.state.position[order.symbol] = self.state.position.get(order.symbol, 0) - volume
        self.back_data.profit_loss[order.symbol] += price * volume
        order.quantity += volume
        return Trade(order.symbol, price, volume, buyer,"SUBMISSION", self.state.timestamp)

    def __can_match_sell_order(self, order: Order, market_trade: MarketTrade) -> bool:
        if market_trade.buy_quantity == 0:
            return False
        if market_trade.trade.price < order.price:
            return False
        if market_trade.trade.price == order.price:
            return self.trade_matching_mode == TradeMatchingMode.all
        return True

    # deduct volume from orders. If the volume becomes 0 after deduction, then remove the order from the order dict.
    # orders is a dict of price -> volume.
    def __deduct_volume_from_order(self, orders: dict[int, int], price: int, volume_to_be_deducted: int):
        if orders[price] > 0:   # volume is positive in buy orders, so need to minus
            orders[price] -= volume_to_be_deducted
        elif orders[price] < 0: # volume is negative in sell orders, so need to plus
            orders[price] += volume_to_be_deducted

        if orders[price] == 0:
            orders.pop(price)


    def __match_buy_order_from_price_depth(self, order: Order) -> list[Trade]:
        trades = []
        sell_price_depth = self.state.order_depths[order.symbol].sell_orders
        price_matched = sorted(price for price in sell_price_depth.keys() if price <= order.price)
        for price in price_matched:
            volume = min(order.quantity, abs(sell_price_depth[price]))
            self.__deduct_volume_from_order(sell_price_depth, price, volume)
            trade = self.__create_buy_order(order, volume, price, "")
            trades.append(trade)
            if order.quantity == 0:
                return trades
        return trades


    def __match_sell_order_from_price_depth(self, order: Order) -> list[Trade]:
        trades = []
        buy_price_depth = self.state.order_depths[order.symbol].buy_orders
        price_matches = sorted((price for price in buy_price_depth.keys() if price >= order.price), reverse=True)
        for price in price_matches:
            volume = min(abs(order.quantity), buy_price_depth[price])
            self.__deduct_volume_from_order(buy_price_depth, price, volume)
            trade = self.__create_sell_order(order, volume, price, "")
            trades.append(trade)
            if order.quantity == 0:
                return trades
        return trades

    def __match_buy_order_from_market_trades(self, order, market_trades) -> list[Trade]:
        trades = []
        if self.trade_matching_mode != TradeMatchingMode.none:
            # try match from market_trades
            matched_market_trades = [trade for trade in market_trades if self.__can_match_buy_order(order, trade)]
            for market_trade in matched_market_trades:
                volume = min(order.quantity, market_trade.sell_quantity)
                market_trade.sell_quantity -= volume
                # market_trade.sell_quantity = 0
                trade = self.__create_buy_order(order, volume, order.price, market_trade.trade.seller)
                trades.append(trade)
                if order.quantity == 0:
                    return trades

        return trades

    def __match_sell_order_from_market_trades(self, order, market_trades) -> list[Trade]:
        trades = []
        if self.trade_matching_mode != TradeMatchingMode.none:
            matched_market_trades = [trade for trade in market_trades if self.__can_match_sell_order(order, trade)]
            for market_trade in matched_market_trades:
                volume = min(abs(order.quantity), market_trade.buy_quantity)
                market_trade.buy_quantity -= volume
                # market_trade.buy_quantity = 0
                trade = self.__create_sell_order(order, volume, order.price, market_trade.trade.buyer)
                trades.append(trade)
                if order.quantity == 0:
                    return trades

        return trades