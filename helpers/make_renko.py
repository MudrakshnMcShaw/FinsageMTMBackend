# import pandas as pd
# from stocktrends import Renko
# from logger_setup import logger
# import math

# import psutil, os

# def log_mem(stage):
#     process = psutil.Process(os.getpid())
#     mem = process.memory_info().rss / (1024 ** 2)
#     print(f"{stage}: {mem:.2f} MB")
# # ==================== USER SETTINGS ====================
# # brick_type = 'close'           # 'close' or 'ohlc'
# # brick_method = 'percentage'          # 'atr', 'percentage', or 'traditional'
# # brick_value = 0.5         # ATR period / percentage value / absolute value


# def calculate_brick_size(df, method: str, value: float):
#     """Calculate brick size based on method"""
#     if method == 'atr':
#         # convert the value to int
#         int_value = value
        
#         window = int(math.ceil(int_value)) * 2
#         df_copy = df.tail(window).copy()
        
#         df_copy['h-l'] = df_copy['high'] - df_copy['low']
#         df_copy['h-pc'] = abs(df_copy['high'] - df_copy['close'].shift(1))
#         df_copy['l-pc'] = abs(df_copy['low'] - df_copy['close'].shift(1))
#         df_copy['tr'] = df_copy[['h-l', 'h-pc', 'l-pc']].max(axis=1)
#         atr = df_copy['tr'].rolling(window=int(value)).mean().iloc[-1]
#         brick_size = round(atr)
        
#     elif method == 'percentage':
#         recent_close = abs(df['close'].iloc[-1])
#         print(f"close last: {recent_close}")
#         brick_size = round((value / 100) * recent_close)
        
#     elif method == 'traditional':
#         brick_size = round(value)
        
#     else:
#         raise ValueError("Method must be 'atr', 'percentage', or 'traditional'")
    
#     return max(1, brick_size)


# def get_close_based_renko(df, brick_size):
#     """
#     Manual close-based Renko implementation.
#     Only uses close prices to form bricks.
#     """
#     renko_bars = []
    
#     if len(df) == 0:
#         return pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close'])
    
#     # Initialize with first close
#     current_brick_price = df['close'].iloc[0]
#     current_direction = None  # Will be set on first brick
    
#     for idx, row in df.iterrows():
#         close = row['close']
        
#         # Calculate how many bricks to form
#         price_diff = close - current_brick_price
#         num_bricks = int(abs(price_diff) / brick_size)
        
#         if num_bricks > 0:
#             direction = 1 if price_diff > 0 else -1
            
#             for _ in range(num_bricks):
#                 brick_open = current_brick_price
#                 brick_close = current_brick_price + (brick_size * direction)
                
#                 renko_bars.append({
#                     'date': row['date'],
#                     'open': brick_open,
#                     'high': max(brick_open, brick_close),
#                     'low': min(brick_open, brick_close),
#                     'close': brick_close
#                 })
                
#                 current_brick_price = brick_close
    
#     return pd.DataFrame(renko_bars)


# def fix_duplicate_dates(df):

#     # ensure datetime
#     df["date"] = pd.to_datetime(df["date"])

#     # count duplicates per timestamp
#     dup_index = df.groupby("date").cumcount()

#     # add milliseconds offset
#     df["date"] = df["date"] + pd.to_timedelta(dup_index, unit="s")

#     return df

# def generate_renko(
#     df: pd.DataFrame,
#     brick_type: str = "close",       # "close" or "ohlc"
#     method: str = "percentage",
#     value: float = 0.5
# ):
#     """
#     df must have columns: time, open, high, low, close
#     """
#     df = df.copy()
#     # logger.debug(df.info())
        
#     if "time" in df.columns:
#         df.rename(columns={"time": "date"}, inplace=True)
#     elif "timestamp" in df.columns:
#         df.rename(columns={"timestamp": "date"}, inplace=True)

#     if "date" not in df.columns:
#         raise ValueError(f"No time/date column found. Columns: {df.columns}")
        
#     df["date"] = pd.to_datetime(df["date"], unit="ms")

#     brick_size = calculate_brick_size(df, method, value)
#     if brick_type == "close":
#         # renko_df = get_close_based_renko(df, brick_size)
#         df['high']=df[['open','close']].max(axis=1)
#         df['low']=df[['open','close']].min(axis=1)

#         renko = Renko(df[["date", "open", "high", "low", "close"]])
#         renko.brick_size = brick_size
#         log_mem("before renko.get_ohlc_data()")
#         print(f"renko bric size: {renko.brick_size}")
#         renko_df = renko.get_ohlc_data()
#         log_mem("after renko.get_ohlc_data()")

#     elif brick_type == "ohlc":
#         renko = Renko(df[["date", "open", "high", "low", "close"]])
#         renko.brick_size = brick_size
#         log_mem("before renko.get_ohlc_data()")
#         renko_df = renko.get_ohlc_data()
#         log_mem("after renko.get_ohlc_data()")

#     else:
#         raise ValueError("brick_type must be 'close' or 'ohlc'")
#     renko_final = fix_duplicate_dates(renko_df)
#     renko_final.to_csv("renko_csv", index=False)
#     return renko_final, brick_size


import pandas as pd
import numpy as np
import math


# ==================== BRICK SIZE CALCULATION ====================

def calculate_brick_size(df: pd.DataFrame, method: str, value: float, margin: float) -> int:
    """Calculate brick size based on method — vectorized, no row iteration."""
    # if method == 'atr':
    #     window = int(math.ceil(value)) * 2
    #     tail = df.tail(window)
    #     high = tail['high'].values
    #     low  = tail['low'].values
    #     prev_close = tail['close'].shift(1).values

    #     tr = np.maximum.reduce([
    #         high - low,
    #         np.abs(high - prev_close),
    #         np.abs(low  - prev_close)
    #     ])
    #     atr = pd.Series(tr).rolling(window=int(value)).mean().iloc[-1]
    #     brick_size = round(atr)

    if method == 'percentage':
        if margin is not None:
            recent_close = abs(df['close'].iloc[-1])
        else:
            recent_close = float(margin)
        brick_size = round((value / 100) * recent_close)

    elif method == 'traditional':
        brick_size = round(value)

    else:
        raise ValueError("method must be 'atr', 'percentage', or 'traditional'")

    return max(1, brick_size)


# ==================== FAST NUMPY RENKO ====================

def _build_renko_numpy(closes: np.ndarray, dates: np.ndarray, brick_size: float):
    """
    Pure NumPy Renko builder — O(n) single pass, no Python-level loop overhead
    beyond the number of *bricks* formed (not candles).
    """
    n = len(closes)
    if n == 0:
        return pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close'])

    # Pre-allocate output arrays (worst case: every candle forms a brick)
    max_bricks = n * 2          # generous upper bound
    out_date  = np.empty(max_bricks, dtype=dates.dtype)
    out_open  = np.empty(max_bricks, dtype=np.float64)
    out_close = np.empty(max_bricks, dtype=np.float64)

    brick_price = closes[0]
    count = 0

    for i in range(1, n):
        close = closes[i]
        diff  = close - brick_price
        steps = int(abs(diff) / brick_size)

        if steps == 0:
            continue

        direction = 1 if diff > 0 else -1
        step_val  = brick_size * direction

        for _ in range(steps):
            new_price = brick_price + step_val
            out_date[count]  = dates[i]
            out_open[count]  = brick_price
            out_close[count] = new_price
            brick_price      = new_price
            count           += 1

    # Trim to actual size
    out_date  = out_date[:count]
    out_open  = out_open[:count]
    out_close = out_close[:count]

    renko_df = pd.DataFrame({
        'date':  out_date,
        'open':  out_open,
        'high':  np.maximum(out_open, out_close),
        'low':   np.minimum(out_open, out_close),
        'close': out_close,
    })
    return renko_df


# ==================== DUPLICATE DATE FIX ====================

def fix_duplicate_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    dup_index  = df.groupby("date").cumcount()
    df["date"] = df["date"] + pd.to_timedelta(dup_index, unit="s")
    return df


# ==================== PUBLIC API ====================

def generate_renko(
    df: pd.DataFrame,
    brick_type: str  = "close",
    method: str      = "percentage",
    value: float     = 0.5,
    save_csv: bool   = False,
    margin: float = 1000000
) -> tuple[pd.DataFrame, int]:
    """
    Convert OHLC candles to Renko bars.

    Parameters
    ----------
    df         : DataFrame with columns time/timestamp/date, open, high, low, close
    brick_type : 'close' (use only close prices) or 'ohlc' (use high/low extremes)
    method     : 'atr' | 'percentage' | 'traditional'
    value      : ATR period / percentage / absolute value
    save_csv   : write renko_csv to disk when True

    Returns
    -------
    (renko_df, brick_size)
    """
    df = df.copy()

    # Normalise timestamp column name
    for col in ("time", "timestamp"):
        if col in df.columns:
            df.rename(columns={col: "date"}, inplace=True)
            break

    if "date" not in df.columns:
        raise ValueError(f"No time/date column found. Columns: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"], unit="ms")

    brick_size = calculate_brick_size(df, method, value, margin)

    if brick_type == "close":
        closes = df["close"].values.astype(np.float64)
        dates  = df["date"].values
        renko_df = _build_renko_numpy(closes, dates, brick_size)

    elif brick_type == "ohlc":
        # For OHLC mode: interleave highs and lows so each candle can form
        # both up and down bricks — standard OHLC Renko convention.
        # We alternate: if previous brick was up → check low first, then high
        # (conservative). Simple fast approach: use (high+low)/2 mid-prices.
        mids  = ((df["high"] + df["low"]) / 2).values.astype(np.float64)
        dates = df["date"].values
        renko_df = _build_renko_numpy(mids, dates, brick_size)

    else:
        raise ValueError("brick_type must be 'close' or 'ohlc'")

    renko_final = fix_duplicate_dates(renko_df)

    # if save_csv:
    #     renko_final.to_csv("renko_csv", index=False)

    return renko_final, brick_size