import sys
from datetime import datetime
from typer import Argument, Option, Typer
from typing import Annotated, Optional
from pathlib import Path
from prosperity4bt.back_tester import BackTester
from prosperity4bt.models.test_options import TestOptions, TradeMatchingMode

app = Typer(context_settings={"help_option_names": ["--help", "-h"]})

@app.command()
def run(
    algorithm: Annotated[Path, Argument(help="Path to the Python file containing the algorithm to backtest.", show_default=False,exists=True, file_okay=True, dir_okay=False, resolve_path=True)],
    days: Annotated[list[str], Argument(help="The days to backtest on. <round>-<day> for a single day, <round> for all days in a round.", show_default=False)],
    out: Annotated[Optional[Path], Option(help="File to save output log to (defaults to backtests/<timestamp>.log).", show_default=False, dir_okay=False, resolve_path=True)] = None,
    no_out: Annotated[bool, Option("--no-out", help="Skip saving output log.")] = False,
    data: Annotated[Optional[Path], Option(help="Path to data directory.", show_default=False, exists=True, file_okay=False, dir_okay=True, resolve_path=True)] = None,
    print_output: Annotated[bool, Option("--print", help="Print the trader's output to stdout while it's running.")] = False,
    match_trades: Annotated[TradeMatchingMode, Option(help="How to match orders against market trades. 'all' matches trades with prices equal to or worse than your quotes, 'worse' matches trades with prices worse than your quotes, 'none' does not match trades against orders at all.")] = TradeMatchingMode.server_like,
    no_progress: Annotated[bool, Option("--no-progress", help="Don't show progress bars.")] = False,
    no_merge_pnl: Annotated[bool, Option("--no-merge-pnl", help="Merge profit and loss across days.")] = False,
    no_vis: Annotated[bool, Option("--no-vis", help="Open backtest results in https://kevin-fu1.github.io/imc-prosperity-4-visualizer/ when done.")] = False,
    original_timestamps: Annotated[bool, Option("--original-timestamps", help="Preserve original timestamps in output log rather than making them increase across days.")] = False,
    no_maf: Annotated[bool, Option("--no-maf", help="Simulate NOT winning the MAF: multiply all LOB and market-trade volumes by 0.8 (default = full market = winning MAF).")] = False,
    same_tick_trades: Annotated[bool, Option("--same-tick-trades", help="Pre-load current tick's market trades into state.market_trades before the bot runs, matching live simulation behavior. Default: off (one-tick lag, legacy behavior).")] = False,
):
    if out is not None and no_out:
        print("Error: --out and --no-out are mutually exclusive")
        sys.exit(1)

    options = TestOptions(algorithm, days, __parse_out(out, no_out))
    options.back_data_dir = data
    options.print_output = print_output
    options.trade_matching_mode = match_trades
    options.show_progress = not no_progress
    options.merge_profit_loss = not no_merge_pnl
    options.show_visualizer = not no_vis
    options.merge_timestamps = not original_timestamps
    options.maf_factor = 0.8 if no_maf else 1.0
    options.same_tick_trades = same_tick_trades

    back_tester = BackTester(options)
    back_tester.run()


def __parse_out(out: Optional[Path], no_out: bool) -> Optional[Path]:
    if out is not None:
        return out

    if no_out:
        return None

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return Path.cwd() / "backtests" / f"{timestamp}.log"


def main() -> None:
    app()

if __name__ == "__main__":
    main()