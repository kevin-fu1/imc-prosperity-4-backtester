import sys
from importlib import import_module, reload
from pathlib import Path
from typing import Any, Optional
from prosperity4bt.tools.data_reader import BackDataReader, PackageResourcesReader
from prosperity4bt.models.output import BacktestResult
from prosperity4bt.tools.output_file_writer import OutputFileWriter
from prosperity4bt.tools.result_merger import ResultMerger
from prosperity4bt.tools.summary_printer import SummaryPrinter
from prosperity4bt.models.test_options import TestOptions, RoundDayOption
from prosperity4bt.test_runner import TestRunner
from prosperity4bt.tools.visualizer import Visualizer


class BackTester:
    def __init__(self, options: TestOptions):
        self.options = options

    def run(self):
        print(f"running algorithm '{self.options.algorithm_path}'...")

        trader_module = self.__load_algorithm()
        data_reader = self.__get_data_reader(self.options.back_data_dir)
        round_days_options = RoundDayOption.parse(self.options.round_day, data_reader)
        merger = ResultMerger(self.options.merge_timestamps, self.options.merge_profit_loss)

        results = []
        for round in round_days_options:
            for day in round.days:
                print(f"Backtesting {self.options.algorithm_path} for round: {round.round} day: {day}")
                result = self.__run_test(trader_module, data_reader, round.round, day)
                results.append(result)
                SummaryPrinter.print_day_summary(result)

            if len(round.days) > 1:
                SummaryPrinter.print_overall_summary(results)

        merged_result = merger.merge(results)

        if self.options.output_file is not None:
            OutputFileWriter.write_to_file(self.options.output_file, merged_result)
            print(f"\nSuccessfully saved backtest results to {self.__format_path(self.options.output_file)}")

            if self.options.show_visualizer:
                self.__open_visualizer()


    def __load_algorithm(self) -> Any:
        try:
            sys.path.append(str(self.options.algorithm_path.parent))
            trader_module = import_module(self.options.algorithm_path.stem)
        except ModuleNotFoundError as e:
            print(f"{self.options.algorithm_path} is not a valid algorithm file: {e}")
            sys.exit(1)

        if not hasattr(trader_module, "Trader"):
            print(f"{self.options.algorithm_path} does not expose a Trader class")
            sys.exit(1)

        return trader_module


    def __get_data_reader(self, data_dir: Optional[Path]) -> BackDataReader:
        if data_dir is not None:
            from prosperity4bt.tools.data_reader import FileSystemReader
            return FileSystemReader(data_dir)
        else:
            return PackageResourcesReader()


    def __run_test(self, trader_module, data_reader: BackDataReader, round: int, day: int) -> BacktestResult:
        reload(trader_module)
        test_runner = TestRunner(
            trader_module.Trader(),
            data_reader,
            round,
            day,
            self.options.show_progress,
            self.options.print_output,
            self.options.trade_matching_mode)
        result = test_runner.run()
        return result


    def __format_path(self, path: Path) -> str:
        cwd = Path.cwd()
        if path.is_relative_to(cwd):
            return str(path.relative_to(cwd))
        else:
            return str(path)


    def __open_visualizer(self):
        visualizer = Visualizer()
        visualizer.open(self.options.output_file)
