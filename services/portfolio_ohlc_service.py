from fastapi import HTTPException
from datetime import datetime
from logger_setup import logger  
import pandas as pd

def get_portfolio_ohlc(
    portfolio_name,
    db
):
    try:
        # 1. Get strategies + lots
        portfolio = db.portfolios.find_one(
            {"portfolio": portfolio_name},
            {"_id": 0, "strategies.strategy": 1, "strategies.lots": 1}
        )
        if not portfolio:
            raise HTTPException(404, "Portfolio not found")

        lots_map = {}
        strategy_names = []
        for s in portfolio.get("strategies", []):
            name = s.get("strategy")
            if name:
                lots_map[name] = s.get("lots", 1)
                strategy_names.append(name)

        if not strategy_names:
            raise HTTPException(400, "No strategies in portfolio")

        # 2. Fetch data
        cursor = (
            db.strategies_mtm_data
            .find(
                {"strategy": {"$in": strategy_names}},
                {"Date": 1, "CumulativePnl": 1, "strategy": 1}
            )
            .batch_size(50000) 
        )
        docs = list(cursor)
        if not docs:
            return {"portfolio": portfolio_name, "ohlc": []}

        df = pd.DataFrame(docs)

        # 1️⃣ Convert Date to datetime
        df["Date"] = pd.to_datetime(df["Date"], utc=True)

        # 2️⃣ Apply lots on Cumulative PnL (NOT diff)
        df["scaled_cumul"] = df["CumulativePnl"] * df["strategy"].map(lots_map)

        # 3️⃣ Pivot → align all strategies on all timestamps
        pivot = df.pivot_table(
            index="Date",
            columns="strategy",
            values="scaled_cumul",
            aggfunc="last"
        )

        # 4️⃣ Forward fill missing cumulative PnL for each strategy
        pivot = pivot.ffill()

        # 5️⃣ Portfolio cumulative PnL = sum of all strategies horizontally
        pivot["portfolio_equity"] = pivot.sum(axis=1)

        equity = pivot["portfolio_equity"].reset_index()

        equity["open"] = equity["portfolio_equity"].shift(1).fillna(equity["portfolio_equity"])
        equity["close"] = equity["portfolio_equity"].fillna(equity["portfolio_equity"])

        # High/Low
        equity["high"] = equity[["open", "close"]].max(axis=1)
        equity["low"]  = equity[["open", "close"]].min(axis=1)

        # 7️⃣ Convert UNIX time (IST)
        equity["time"] = ((equity["Date"].astype("int64") // 10**9) - 19800) * 1000
        filename = f"{portfolio_name}_csv_{datetime.utcnow():%Y-%m-%d}.csv"

        equity[["time", "open", "high", "low", "close"]].to_csv(
            filename,
            index=False
        )
        out = equity[["time", "open", "high", "low", "close"]].to_dict("records")

        return out
    except Exception as e:
        logger.exception(f"Error while generating OHLC for portfolio '{portfolio_name}'")
        raise HTTPException(status_code=500, detail=str(e))