"""
FILE 1: BASE CODE - nifty50_base.py
This is the core file that fetches stock data
All other files will use this as a base

WHAT THIS DOES IN SIMPLE TERMS:
1. Connects to Yahoo Finance website
2. Gets stock prices for all 50 Nifty companies
3. Saves the data in a JSON file with today's date (./nifty50_data)
4. Writes a daily log file (./logs/nifty50_YYYY-MM-DD.log)
"""
import yfinance as yf
import json
from datetime import datetime, timedelta
import time
import sys
import os
import tempfile
import shutil
import logging

# -------- Paths (JSON -> nifty50_data, LOGS -> logs) --------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

OUTPUT_DIR = os.path.join(BASE_DIR, "nifty50_data")   # JSON files live here
os.makedirs(OUTPUT_DIR, exist_ok=True)

LOG_DIR = os.path.join(BASE_DIR, "logs")               # Log files live here
os.makedirs(LOG_DIR, exist_ok=True)

# one log per day, e.g., logs/nifty50_2025-11-10.log
LOG_PATH = os.path.join(LOG_DIR, f"nifty50_{datetime.now().strftime('%Y-%m-%d')}.log")

# Re-init logging to write into LOG_DIR
for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)

logging.basicConfig(
    filename=LOG_PATH,  # <-- write logs into ./logs/<dated>.log
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info(f"cwd={os.getcwd()} | basedir={BASE_DIR} | outdir={OUTPUT_DIR} | logdir={LOG_DIR} | py={sys.version.split()[0]}")

# ============================================================================
# NIFTY 50 STOCK SYMBOLS (as of November 2024)
# Update this list if Nifty 50 composition changes (check NSE website)
# ============================================================================

NIFTY_50_SYMBOLS = [
    'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
    'HINDUNILVR.NS', 'ITC.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'KOTAKBANK.NS',
    'LT.NS', 'AXISBANK.NS', 'ASIANPAINT.NS', 'MARUTI.NS', 'TITAN.NS',
    'SUNPHARMA.NS', 'ULTRACEMCO.NS', 'BAJFINANCE.NS', 'NESTLEIND.NS', 'HCLTECH.NS',
    'WIPRO.NS', 'POWERGRID.NS', 'NTPC.NS', 'TATAMOTORS.NS', 'TATASTEEL.NS',
    'M&M.NS', 'BAJAJFINSV.NS', 'TECHM.NS', 'ADANIENT.NS', 'ONGC.NS',
    'COALINDIA.NS', 'DIVISLAB.NS', 'GRASIM.NS', 'HINDALCO.NS', 'INDUSINDBK.NS',
    'JSWSTEEL.NS', 'BRITANNIA.NS', 'CIPLA.NS', 'EICHERMOT.NS', 'HEROMOTOCO.NS',
    'DRREDDY.NS', 'APOLLOHOSP.NS', 'BPCL.NS', 'ADANIPORTS.NS', 'TATACONSUM.NS',
    'BAJAJ-AUTO.NS', 'SHRIRAMFIN.NS', 'SBILIFE.NS', 'LTIM.NS', 'BEL.NS'
]

# ============================================================================
# NSE MARKET HOLIDAYS - 2025
# Source: https://www.nseindia.com/regulations/holiday-list
# Update this list every year in January
# ============================================================================

NSE_HOLIDAYS_2025 = [
    # January
    '2025-01-26',
    # February
    '2025-02-26',
    # March
    '2025-03-14', '2025-03-31',
    # April
    '2025-04-10', '2025-04-14', '2025-04-18',
    # May
    '2025-05-01',
    # June
    '2025-06-07',
    # July
    '2025-07-07',
    # August
    '2025-08-15', '2025-08-16', '2025-08-27',
    # October
    '2025-10-02', '2025-10-21', '2025-10-22', '2025-10-23',
    # November
    '2025-11-05',
    # December
    '2025-12-25',
]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def is_market_open(date_str):
    """Helper: weekend/holiday check (used for fallback date only)."""
    if date_str in NSE_HOLIDAYS_2025:
        logging.info(f"NSE Holiday: {date_str}")
        return False
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    if date_obj.weekday() in [5, 6]:  # Saturday, Sunday
        logging.info(f"Weekend: {date_str}")
        return False
    return True

def get_actual_trading_date():
    """
    Get actual trading date from market data (not system date).
    Falls back to weekend/holiday logic if Yahoo call fails.
    """
    try:
        logging.info("Fetching actual trading date...")
        sample = yf.Ticker('RELIANCE.NS')
        history = sample.history(period='5d')
        if not history.empty:
            actual_date = history.index[-1]
            date_str = actual_date.strftime('%Y-%m-%d')
            logging.info(f"Actual trading date from data: {date_str}")
            return date_str
    except Exception as e:
        logging.warning(f"Could not get actual trading date: {e}")

    today = datetime.now()
    for days_back in range(7):
        check_date = today - timedelta(days=days_back)
        date_str = check_date.strftime('%Y-%m-%d')
        if is_market_open(date_str):
            logging.info(f"Calculated trading date: {date_str}")
            return date_str
    return today.strftime('%Y-%m-%d')

def validate_stock_data(stock_data):
    """Basic sanity validation for OHLC values."""
    try:
        if any(v is None for v in [stock_data['open'], stock_data['high'], stock_data['low'], stock_data['close']]):
            return False
        if any(v <= 0 for v in [stock_data['open'], stock_data['high'], stock_data['low'], stock_data['close']]):
            return False
        if stock_data['high'] < stock_data['low']:
            return False
        if stock_data['close'] > 100000:
            return False
        if stock_data['volume'] == 0:
            logging.warning(f"{stock_data['symbol']}: Zero volume")
        return True
    except Exception as e:
        logging.error(f"Validation error: {e}")
        return False

def fetch_with_retry(symbol, max_retries=2, per_symbol_timeout=15):
    """Fetch stock history with retry and per-symbol timeout."""
    start = time.time()
    for attempt in range(max_retries):
        if time.time() - start > per_symbol_timeout:
            logging.warning(f"{symbol}: Timeout after {per_symbol_timeout}s")
            return None
        try:
            stock = yf.Ticker(symbol)
            history = stock.history(period='5d')
            if not history.empty:
                return history
            if attempt < max_retries - 1:
                logging.warning(f"{symbol}: Empty data, retrying in 2s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(2)
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"{symbol}: Error '{e}', retrying... (attempt {attempt + 1}/{max_retries})")
                time.sleep(2)
            else:
                logging.error(f"{symbol}: Failed after {max_retries} attempts: {e}")
                return None
    return None

# ============================================================================
# MAIN FUNCTIONS
# ============================================================================

def fetch_stock_data():
    """
    Fetch all Nifty 50 stocks, validate, and build the payload.
    fetch_date is corrected from actual rows after the loop.
    """
    logging.info("=" * 70)
    logging.info("STARTING NIFTY 50 DATA FETCH")
    logging.info("=" * 70)

    print("=" * 70)
    print("NIFTY 50 DATA FETCHER - ROBUST VERSION")
    print("=" * 70)

    date_string = get_actual_trading_date()  # indicative only

    print(f" Trading Date (indicative): {date_string}")
    print(f" Fetch Time: {datetime.now().strftime('%H:%M:%S')}")
    logging.info(f"Fetching (initial target): {date_string}")

    now_hm = int(datetime.now().strftime("%H%M"))  # e.g., 1637
    market_status = "open" if 915 <= now_hm <= 1530 else "closed"

    all_data = {
        "schema_version": "1.0",
        "fetch_date": date_string,  # will be corrected after loop
        "fetch_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "market_status": market_status,
        "total_stocks": 0,
        "stocks": []
    }

    success_count = 0
    fail_count = 0
    invalid_count = 0

    start_time = time.time()

    print("\nFetching stocks:")
    print("-" * 70)

    import random  # okay here
    for i, symbol in enumerate(NIFTY_50_SYMBOLS, 1):
        try:
            print(f"[{i:2d}/50] {symbol:20s} ", end="")
            history = fetch_with_retry(symbol)
            if history is not None and not history.empty:
                latest = history.iloc[-1]
                stock_data = {
                    "symbol": symbol.replace('.NS', ''),
                    "company_name": symbol.replace('.NS', ''),
                    "date": history.index[-1].strftime('%Y-%m-%d'),
                    "open": round(float(latest['Open']), 2),
                    "high": round(float(latest['High']), 2),
                    "low": round(float(latest['Low']), 2),
                    "close": round(float(latest['Close']), 2),
                    "volume": int(latest['Volume'])
                }
                if validate_stock_data(stock_data):
                    all_data["stocks"].append(stock_data)
                    success_count += 1
                    print(f" {stock_data['close']:>8.2f}")
                    logging.info(f"{symbol}: Success - {stock_data['close']}")
                else:
                    invalid_count += 1
                    print("Invalid data")
                    logging.warning(f"{symbol}: Invalid data, rejected")
            else:
                fail_count += 1
                print(" No data")
                logging.warning(f"{symbol}: No data available")
            time.sleep(0.25 + random.uniform(0, 0.15))  # jitter
        except Exception as error:
            fail_count += 1
            print(f" Error: {str(error)[:30]}")
            logging.error(f"{symbol}: Error - {str(error)}")

    elapsed = time.time() - start_time
    all_data["total_stocks"] = success_count

    # Correct fetch_date from actual rows
    if all_data["stocks"]:
        all_data["fetch_date"] = max(s["date"] for s in all_data["stocks"])

    print("-" * 70)
    print("\n SUMMARY:")
    print(f"    Successful:  {success_count:2d}/50")
    print(f"     Failed:      {fail_count:2d}/50")
    print(f"     Invalid:     {invalid_count:2d}/50")
    print(f"     Time taken:  {elapsed:.1f}s")
    print("=" * 70)

    logging.info(f"Summary: {success_count} success, {fail_count} failed, {invalid_count} invalid")
    logging.info(f"Time taken: {elapsed:.1f}s")
    logging.info(f"Final fetch_date set to: {all_data['fetch_date']}")

    return all_data

def save_to_json_atomic(data, filename=None):
    """Save JSON with atomic write in OUTPUT_DIR."""
    if filename is None:
        filename = f"nifty50_{data['fetch_date']}.json"

    final_path = os.path.join(OUTPUT_DIR, filename)
    temp_path = final_path + ".tmp"

    if os.path.exists(final_path):
        logging.info(f"File exists, overwriting: {final_path}")
        print(f"\n  File exists, overwriting: {final_path}")

    try:
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        shutil.move(temp_path, final_path)

        file_size = os.path.getsize(final_path) / 1024  # KB
        print(f"\n File saved successfully!")
        print(f"    Name: {os.path.basename(final_path)}")
        print(f"    Path: {final_path}")
        print(f"    Stocks: {data['total_stocks']}/50")
        print(f"    Size: {file_size:.1f} KB")

        logging.info(f"File saved: {final_path} ({file_size:.1f} KB)")
        return final_path

    except Exception as e:
        logging.error(f"Failed to save file: {e}")
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass
        raise

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    logging.info("\n" + "="*70)
    logging.info("SCRIPT EXECUTION STARTED")
    logging.info("="*70)
    
    print("\n NIFTY 50 AUTOMATIC FETCHER\n")
    
    try:
        stock_data = fetch_stock_data()
        
        if stock_data["total_stocks"] == 0:
            print("\n CRITICAL ERROR: No stocks fetched!")
            print("\nPossible reasons:")
            print("  • No internet connection")
            print("  • Yahoo Finance is down")
            print("  • All stocks failed validation")
            print("  • Market data not yet available")
            print(f"\nCheck log: {LOG_PATH}")
            
            logging.critical("No stocks fetched - exiting with error")
            sys.exit(1)
        
        if stock_data["total_stocks"] < 40:
            print(f"\n  WARNING: Only {stock_data['total_stocks']}/50 stocks fetched")
            print("   Continuing anyway, but check the log for issues")
            logging.warning(f"Low success rate: {stock_data['total_stocks']}/50")
        
        json_file = save_to_json_atomic(stock_data)
        
        print(f"\n SUCCESS!")
        print(f"    JSON: {os.path.abspath(json_file)}")
        print(f"    LOG : {LOG_PATH}")
        print()
        
        logging.info("Script completed successfully")
        sys.exit(0)
    
    except KeyboardInterrupt:
        print("\n\n  Interrupted by user (Ctrl+C)")
        logging.warning("Script interrupted by user")
        sys.exit(1)
    
    except Exception as e:
        print(f"\n FATAL ERROR: {str(e)}")
        print(f"Check log: {LOG_PATH}")
        logging.critical(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)
