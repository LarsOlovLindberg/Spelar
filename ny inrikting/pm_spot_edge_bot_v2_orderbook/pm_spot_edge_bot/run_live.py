"C:\Users\lars-\pm_spot_edge_bot\run_live.py"
import os
import time
from src.config import Settings
from src.runner import LiveRunner

def main() -> None:
    s = Settings.from_env()
    runner = LiveRunner(settings=s)
    print("LIVE MODE")
    print(f"Kraken pair: {s.kraken_pair}")
    print(f"PM slug: {s.pm_market_slug}")
    print(f"PM side: {s.pm_side}")
    print(f"Poll secs: {s.poll_secs}")
    print("Press Ctrl+C to stop.")
    while True:
        runner.step()
        time.sleep(s.poll_secs)

if __name__ == "__main__":
    main()
