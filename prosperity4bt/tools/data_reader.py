from abc import abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from importlib import resources
from pathlib import Path
from typing import ContextManager, Optional
from prosperity4bt.datamodel import Trade, Symbol
from prosperity4bt.models.input import PriceRow, ObservationRow, BacktestData



class BackDataReader:

    def read_from_file(self, round_num: int, day_num: int) -> BacktestData:
        prices = self.__get_prices(round_num, day_num)
        trades = self.__get_trades(round_num, day_num)
        observations = self.__get_observations(round_num, day_num)

        prices_by_timestamp: dict[int, dict[Symbol, PriceRow]] = defaultdict(dict)
        for row in prices:
            prices_by_timestamp[row.timestamp][row.product] = row

        trades_by_timestamp: dict[int, dict[Symbol, list[Trade]]] = defaultdict(lambda: defaultdict(list))
        for trade in trades:
            trades_by_timestamp[trade.timestamp][trade.symbol].append(trade)

        products = sorted(set(row.product for row in prices))
        profit_loss = {product: 0.0 for product in products}

        observations_by_timestamp = {row.timestamp: row for row in observations}

        backtest_data = BacktestData(
            round_num=round_num,
            day_num=day_num,
            prices=prices_by_timestamp,
            trades=trades_by_timestamp,
            observations=observations_by_timestamp,
            products=products,
            profit_loss=profit_loss,
        )
        return backtest_data

    def __get_prices(self, round_num: int, day_num: int):
        prices = []
        with self._read_file_content([f"round{round_num}", f"prices_round_{round_num}_day_{day_num}.csv"]) as file:
            if file is None:
                raise ValueError(f"Prices data is not available for round {round_num} day {day_num}")

            for line in file.read_text(encoding="utf-8").splitlines()[1:]:
                price_row = PriceRow.parse_from_str(line)
                prices.append(price_row)
        return prices

    def __get_trades(self, round_num: int, day_num: int):
        trades = []
        with self._read_file_content([f"round{round_num}", f"trades_round_{round_num}_day_{day_num}.csv"]) as file:
            if file is not None:
                for line in file.read_text(encoding="utf-8").splitlines()[1:]:
                    columns = line.split(";")
                    trades.append(
                        Trade(
                            symbol=columns[3],
                            price=int(float(columns[5])),
                            quantity=int(columns[6]),
                            buyer=columns[1],
                            seller=columns[2],
                            timestamp=int(columns[0]),
                        )
                    )
        return trades

    def __get_observations(self, round_num: int, day_num: int):
        observations = []
        with self._read_file_content([f"round{round_num}", f"observations_round_{round_num}_day_{day_num}.csv"]) as file:
            if file is not None:
                for line in file.read_text(encoding="utf-8").splitlines()[1:]:
                    observations.append(ObservationRow.parse_from_str(line))
        return observations

    @abstractmethod
    def available_days(self, round: int) -> list[int]:
        if round == 0:
            return [-2, -1]
        if round == 1:
            return [-3, -2, -1, 0]
        if round == 2:
            return [-1, 0, 1]
        if round == 3:
            return [0, 1, 2]
        if round == 4:
            return [1, 2, 3]
        if round == 5:
            return [2, 3, 4]
        return []

    @abstractmethod
    def _read_file_content(self, path_parts: list[str]) -> ContextManager[Optional[Path]]:
        """Given a path to a file, yields a single Path object to the file or None if the file does not exist."""
        raise NotImplementedError()


class PackageResourcesReader(BackDataReader):
    def _read_file_content(self, path_parts: list[str]) -> ContextManager[Optional[Path]]:
        try:
            file_path = f"prosperity4bt.resources.{'.'.join(path_parts[:-1])}"
            container = resources.files(file_path)
            file = container / path_parts[-1]
            if not file.is_file():
                return wrap_in_context_manager(None)

            return resources.as_file(file)
        except Exception:
            return wrap_in_context_manager(None)


class FileSystemReader(BackDataReader):
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir

    def _read_file_content(self, path_parts: list[str]) -> ContextManager[Optional[Path]]:
        file_path = self.data_dir.joinpath(*path_parts)
        if file_path.is_file():
            return wrap_in_context_manager(file_path)
        return wrap_in_context_manager(None)


@contextmanager
def wrap_in_context_manager(value):
    yield value