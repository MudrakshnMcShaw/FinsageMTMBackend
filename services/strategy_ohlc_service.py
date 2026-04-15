from fastapi import HTTPException
from logger_setup import logger  
import pandas as pd

def get_strategy_ohlc(
        strategy_name,
        db):
    try:
        logger.info(f"Fetching MTM data for strategy: {strategy_name}")
        cursor = db.strategies_mtm_data.find(
            {
                "strategy": strategy_name,
            },
            {"_id": 0, "Date": 1, "CumulativePnl": 1}
        )

        df = pd.DataFrame(list(cursor))
        # df.to_csv('sorted.csv')
        if df.empty:
            return []
            
        # ---- 2. Convert datetime to UNIX timestamp ---- #
        df["time"] = (df["Date"].astype("int64") // 10**9 -19800) * 1000   # Faster than .timestamp()
        
        # ---- 3. Compute OHLC using vectorized operations ---- #
        # OPEN = previous close or current if it's first row
        df["open"] = df["CumulativePnl"].shift(1).fillna(df["CumulativePnl"])

        # CLOSE = current CumulativePnl
        df["close"] = df["CumulativePnl"]

        # HIGH & LOW
        df["high"] = df[["open", "close"]].max(axis=1)
        df["low"]  = df[["open", "close"]].min(axis=1)
        # df.to_csv('test.csv')
        # ---- 4. Select final required columns ---- #
        out = df[["time", "open", "high", "low", "close"]].to_dict(orient="records")

        logger.info(f"Generated {len(out)} OHLC candles for {strategy_name}")

        return out
    except Exception as e:
        logger.exception(f"Error while generating OHLC for '{strategy_name}'")
        raise HTTPException(status_code=500, detail=str(e))