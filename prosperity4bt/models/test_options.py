from pathlib import Path
from enum import Enum
from prosperity4bt.tools.data_reader import BackDataReader


class TradeMatchingMode(str, Enum):
    all = "all"
    worse = "worse"
    none = "none"


class TestOptions:
    def __init__(self, algorithm_path: Path, round_day: list[str], output_file: Path):
        self.algorithm_path = algorithm_path
        self.round_day = round_day
        self.output_file = output_file
        self.back_data_dir = None
        self.print_output = False
        self.trade_matching_mode = TradeMatchingMode.all
        self.show_progress = False
        self.merge_profit_loss = False
        self.show_visualizer = False
        self.merge_timestamps = True
        self.maf_factor = 1.0
        self.same_tick_trades = False


class RoundDayOption:
    def __init__(self, round: int):
        self.round = round
        self.days = []

    def add_day(self, day):
        self.days.append(day)

    def add_days(self, days: list[int]):
        self.days.extend(days)

    @staticmethod
    def parse(round_day_str: list[str], data_reader: BackDataReader) -> list["RoundDayOption"]:
        options = []

        for arg in round_day_str:
            day_num = None
            if "-" in arg:
                round_num, day_num = map(int, arg.split("-", 1))
            else:
                round_num = int(arg)

            available_days = data_reader.available_days(round_num)

            if day_num is not None and day_num not in available_days:
                print(f"Warning: no data found for round {round_num} day {day_num}")
                continue

            days = [day_num] if day_num is not None else available_days
            if len(days) == 0:
                print(f"Warning: no data found for round {round_num}")
                continue

            option = RoundDayOption(round_num)
            option.add_days(days)
            options.append(option)
        return options

