from prosperity4bt.datamodel import TradingState, Order, Symbol, Trade
from prosperity4bt.models.input import BacktestData, MarketTrade
from prosperity4bt.models.output import TradeRow
from prosperity4bt.models.test_options import TradeMatchingMode


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

            # 1) First match against visible order book
            for order in orders:
                if order.quantity > 0:
                    new_trade = self.__match_buy_order_from_price_depth(order)
                else:
                    new_trade = self.__match_sell_order_from_price_depth(order)

                if len(new_trade) > 0:
                    new_trades.extend(new_trade)

            # 2) Snapshot leftover book AFTER own depth matches
            leftover_buy_depth = self.state.order_depths[product].buy_orders
            leftover_sell_depth = self.state.order_depths[product].sell_orders
            leftover_best_bid = max(leftover_buy_depth.keys()) if leftover_buy_depth else None
            leftover_best_ask = min(leftover_sell_depth.keys()) if leftover_sell_depth else None

            # 3) Match remaining orders against market trades
            buy_orders = [o for o in orders if o.quantity > 0]
            for order in sorted(buy_orders, key=lambda o: o.price, reverse=True):
                if self.trade_matching_mode == TradeMatchingMode.server_like:
                    new_trade = self.__match_buy_order_from_market_trades_server_like(
                        order,
                        market_trades.get(product, []),
                        leftover_best_bid,
                    )
                else:
                    new_trade = self.__match_buy_order_from_market_trades(
                        order,
                        market_trades.get(product, []),
                    )

                if len(new_trade) > 0:
                    new_trades.extend(new_trade)
                    break

            sell_orders = [o for o in orders if o.quantity < 0]
            for order in sorted(sell_orders, key=lambda o: o.price):
                if self.trade_matching_mode == TradeMatchingMode.server_like:
                    new_trade = self.__match_sell_order_from_market_trades_server_like(
                        order,
                        market_trades.get(product, []),
                        leftover_best_ask,
                    )
                else:
                    new_trade = self.__match_sell_order_from_market_trades(
                        order,
                        market_trades.get(product, []),
                    )

                if len(new_trade) > 0:
                    new_trades.extend(new_trade)
                    break

            if len(new_trades) > 0:
                self.state.own_trades[product] = new_trades
                result.extend([TradeRow(trade) for trade in new_trades])

        # adjust market trades, preserving your existing behavior
        for product, trades in market_trades.items():
            trades_updated = False
            for trade in trades:
                if trade.buy_quantity != trade.sell_quantity:
                    trades_updated = True
                    trade.trade.quantity = min(trade.buy_quantity, trade.sell_quantity)

            remaining_market_trades = [
                t.trade
                for t in trades
                if t.trade.quantity > 0 and t.buy_quantity == t.sell_quantity
            ]
            self.state.market_trades[product] = remaining_market_trades
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
        return Trade(order.symbol, price, volume, buyer, "SUBMISSION", self.state.timestamp)

    def __can_match_sell_order(self, order: Order, market_trade: MarketTrade) -> bool:
        if market_trade.buy_quantity == 0:
            return False
        if market_trade.trade.price < order.price:
            return False
        if market_trade.trade.price == order.price:
            return self.trade_matching_mode == TradeMatchingMode.all
        return True

    def __deduct_volume_from_order(self, orders: dict[int, int], price: int, volume_to_be_deducted: int):
        if orders[price] > 0:
            orders[price] -= volume_to_be_deducted
        elif orders[price] < 0:
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

    # -----------------------------
    # Existing market-trade logic
    # -----------------------------
    def __match_buy_order_from_market_trades(self, order, market_trades) -> list[Trade]:
        trades = []
        if self.trade_matching_mode != TradeMatchingMode.none:
            matched_market_trades = [trade for trade in market_trades if self.__can_match_buy_order(order, trade)]
            for market_trade in matched_market_trades:
                volume = min(order.quantity, market_trade.sell_quantity)
                market_trade.sell_quantity -= volume
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
                trade = self.__create_sell_order(order, volume, order.price, market_trade.trade.buyer)
                trades.append(trade)
                if order.quantity == 0:
                    return trades

        return trades

    # -----------------------------
    # New server_like logic
    # -----------------------------
    def __match_buy_order_from_market_trades_server_like(
        self,
        order: Order,
        market_trades: list[MarketTrade],
        leftover_best_bid: int | None,
    ) -> list[Trade]:
        """
        Server-like buy interception:
        - market trade must represent seller hitting bids
        - our bid must strictly improve leftover best bid
        - if we match at all, entire market trade is consumed
        """
        trades = []

        if leftover_best_bid is None:
            return trades

        for market_trade in market_trades:
            if market_trade.sell_quantity <= 0:
                continue

            # Trade must be consistent with a seller hitting the bid side
            if market_trade.trade.price > leftover_best_bid:
                continue

            # Must strictly improve leftover best bid; equal price gets no priority
            if order.price <= leftover_best_bid:
                continue

            # Our bid must still be high enough to trade
            if order.price < market_trade.trade.price:
                continue

            volume = min(order.quantity, market_trade.sell_quantity)
            if volume <= 0:
                continue

            # Keep your server-matching convention: fill at order.price
            trade = self.__create_buy_order(order, volume, order.price, market_trade.trade.seller)
            trades.append(trade)

            # Entire market trade is consumed once it interacts with us at all
            market_trade.sell_quantity = 0
            market_trade.buy_quantity = 0
            market_trade.trade.quantity = 0
            return trades

        return trades

    def __match_sell_order_from_market_trades_server_like(
        self,
        order: Order,
        market_trades: list[MarketTrade],
        leftover_best_ask: int | None,
    ) -> list[Trade]:
        """
        Server-like sell interception:
        - market trade must represent buyer lifting asks
        - our ask must strictly improve leftover best ask
        - if we match at all, entire market trade is consumed
        """
        trades = []

        if leftover_best_ask is None:
            return trades

        for market_trade in market_trades:
            if market_trade.buy_quantity <= 0:
                continue

            # Trade must be consistent with a buyer lifting the ask side
            if market_trade.trade.price < leftover_best_ask:
                continue

            # Must strictly improve leftover best ask; equal price gets no priority
            if order.price >= leftover_best_ask:
                continue

            # Our ask must still be low enough to trade
            if order.price > market_trade.trade.price:
                continue

            volume = min(abs(order.quantity), market_trade.buy_quantity)
            if volume <= 0:
                continue

            # Keep your server-matching convention: fill at order.price
            trade = self.__create_sell_order(order, volume, order.price, market_trade.trade.buyer)
            trades.append(trade)

            # Entire market trade is consumed once it interacts with us at all
            market_trade.buy_quantity = 0
            market_trade.sell_quantity = 0
            market_trade.trade.quantity = 0
            return trades

        return trades