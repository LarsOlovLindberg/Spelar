"C:\Users\lars-\pm_spot_edge_bot\run_backtest.py"
import argparse
from src.config import Settings
from src.runner import BacktestRunner

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--spot", required=True, help="CSV with columns: ts_iso, price")
    ap.add_argument("--pm", required=True, help="CSV with columns: ts_iso, price")
    args = ap.parse_args()

    s = Settings.from_env(backtest=True)
    runner = BacktestRunner(settings=s, spot_csv=args.spot, pm_csv=args.pm)
    report = runner.run()
    print(report.to_text())

if __name__ == "__main__":
    main()
