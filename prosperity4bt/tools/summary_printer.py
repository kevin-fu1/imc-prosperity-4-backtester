from prosperity4bt.models.output import BacktestResult


class SummaryPrinter:

    @staticmethod
    def print_day_summary(result: BacktestResult):
        final_activities = result.final_activities()
        product_lines = [f"{a.symbol}: {a.profit_loss:,.0f}" for a in sorted(final_activities, key=lambda x: x.symbol)]
        total_profit = sum(a.profit_loss for a in final_activities)
        print(*product_lines, sep="\n")
        print(f"Total profit: {total_profit:,.0f}")


    @staticmethod
    def print_overall_summary(results: list[BacktestResult]):
        print("Profit summary:")

        total_profit = 0
        for result in results:
            final_activities = result.final_activities()
            profit = sum(a.profit_loss for a in final_activities)
            print(f"Round {result.round_num} day {result.day_num}: {profit:,.0f}")
            total_profit += profit

        print(f"Total profit: {total_profit:,.0f}")
