import pandas as pd
from stocktrends import Renko

# ==================== USER SETTINGS ====================
# brick_type = 'close'           # 'close' or 'ohlc'
# brick_method = 'percentage'          # 'atr', 'percentage', or 'traditional'
# brick_value = 0.5         # ATR period / percentage value / absolute value


def calculate_brick_size(df, method: str, value: float):
    """Calculate brick size based on method"""
    if method == 'atr':
        df_copy = df.copy()
        df_copy['h-l'] = df_copy['high'] - df_copy['low']
        df_copy['h-pc'] = abs(df_copy['high'] - df_copy['close'].shift(1))
        df_copy['l-pc'] = abs(df_copy['low'] - df_copy['close'].shift(1))
        df_copy['tr'] = df_copy[['h-l', 'h-pc', 'l-pc']].max(axis=1)
        atr = df_copy['tr'].rolling(window=int(value)).mean().iloc[-1]
        brick_size = round(atr)
        
    elif method == 'percentage':
        recent_close = df['close'].iloc[-1]
        brick_size = round((value / 100) * recent_close)
        
    elif method == 'traditional':
        brick_size = round(value)
        
    else:
        raise ValueError("Method must be 'atr', 'percentage', or 'traditional'")
    
    return max(1, brick_size)


def get_close_based_renko(df, brick_size):
    """
    Manual close-based Renko implementation.
    Only uses close prices to form bricks.
    """
    renko_bars = []
    
    if len(df) == 0:
        return pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close'])
    
    # Initialize with first close
    current_brick_price = df['close'].iloc[0]
    current_direction = None  # Will be set on first brick
    
    for idx, row in df.iterrows():
        close = row['close']
        
        # Calculate how many bricks to form
        price_diff = close - current_brick_price
        num_bricks = int(abs(price_diff) / brick_size)
        
        if num_bricks > 0:
            direction = 1 if price_diff > 0 else -1
            
            for _ in range(num_bricks):
                brick_open = current_brick_price
                brick_close = current_brick_price + (brick_size * direction)
                
                renko_bars.append({
                    'time': row['date'],
                    'open': brick_open,
                    'high': max(brick_open, brick_close),
                    'low': min(brick_open, brick_close),
                    'close': brick_close
                })
                
                current_brick_price = brick_close
    
    return pd.DataFrame(renko_bars)

def generate_renko(
    df: pd.DataFrame,
    brick_type: str = "close",       # "close" or "ohlc"
    method: str = "percentage",
    value: float = 0.5
):
    """
    df must have columns: time, open, high, low, close
    """

    df = df.copy()
    
    if "time" in df.columns:
        df.rename(columns={"time": "date"}, inplace=True)
    
    df["date"] = pd.to_datetime(df["date"])

    brick_size = calculate_brick_size(df, method, value)

    if brick_type == "close":
        renko_df = get_close_based_renko(df, brick_size)

    elif brick_type == "ohlc":
        renko = Renko(df[["date", "open", "high", "low", "close"]])
        renko.brick_size = brick_size
        renko_df = renko.get_ohlc_data()

    else:
        raise ValueError("brick_type must be 'close' or 'ohlc'")
    
    return renko_df, brick_size
