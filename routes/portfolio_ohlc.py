from fastapi import APIRouter, HTTPException, Depends, Query
from datetime import datetime
from logger_setup import logger  
import pandas as pd
from database import get_finsage_db

router = APIRouter(prefix="/api", tags=["portfolio"])

def get_db():
    try:
        db = get_finsage_db()
        return db
    except Exception as e:
        logger.error(f"DB unavailable: {e}")
        raise HTTPException(
            status_code=503,
            detail="Finsage Database is down"
        )

@router.get("/portfolio")
def get_portfolios(db=Depends(get_db)):
    """Fetch all available strategies"""
    try:
        logger.info("Fetching list of portfolios from MongoDB...")
        data = list(db.portfolios.find({}, {"_id": 0, "portfolio": 1, "segment": 1, "type": 1}))
        logger.info(f"👍 Fetched {len(data)} porfolios successfully.")
        return data
    
    except Exception as e:
        logger.error(f"❌ Error while fetching portfolio: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_name}/mtmss")
async def get_portfolio_mtm(
    portfolio_name: str,
    db=Depends(get_db)
):
    """
        Build TRUE OHLC for portfolio using EVENT-DRIVEN CUMULATIVE PNL
        Same logic as strategy OHLC (Version 1)
    """

    # 1. Get strategies + lots
    portfolio = db.portfolios.find_one(
        {"portfolio": portfolio_name},
        {"_id": 0, "strategies.strategy": 1, "strategies.lots": 1, "strategies.brokerage": 1, "strategies.slippage": 1}
    )
    if not portfolio:
        raise HTTPException(404, "Portfolio not found")

    lots_map = {}
    brokerage_map = {}
    slippage_map = {}
    strategy_names = []
    for s in portfolio.get("strategies", []):
        name = s.get("strategy")
        if name:
            lots_map[name] = s.get("lots")
            brokerage_map[name] = s.get("brokerage")
            slippage_percent = s.get("slippage")
            slippage_map[name] = slippage_percent / 100
            strategy_names.append(name)
    
    if not strategy_names:
        raise HTTPException(400, "No strategies in portfolio")
    # Costing Logic

    """
    Costing logic:
     1. Calculate brokerage
      - take entry_time (stored as key name in db) by strategy name
      - take lots of that strategy
      - take brokerage of that strategy
      - apply formula = (number of trades of one day) * lots * brokerage
    
    2. Calculate slippage
      - take Entry and Exist Price
      - take lots of strategy
      - take slippage of strategy
      - apply formula, calculated_slippage = (entry price + exit price) * lots * slippage
      - calculated_slippage will the slippage of one trade, we have find for rest of the trade of one day and aggregate them all

    3. Final cost = brokerage + slippage
    4. take adj. pnl =  cumulativePnl - final cost 
    """ 

    trade_logs = (
        db.strategies_trade_logs
        .find(
            {"strategy": {"$in": strategy_names}},
            {"_id": 0, "Key": 1, "strategy": 1, "EntryPrice": 1, "ExitPrice": 1}
        )
        .batch_size(50000)
    )
    trade_docs = list(trade_logs)

    df = pd.DataFrame(trade_docs)
    # df.to_csv('trade_logs.csv')
    df["date"] = df["Key"].dt.date # convert to date only remove time part 00:00..... 

    trade_count = (
        df.groupby(["date", "strategy"])
          .size()
          .reset_index(name="trade_count")
    )

    # Add lots and brokerage
    trade_count["lots"] = trade_count["strategy"].map(lots_map)
    trade_count["brokerage"] = trade_count["strategy"].map(brokerage_map)

    first_date = trade_count["date"].min()
    print("-------------------trade_count after lot and brokerage col add----------------------------")
    print(trade_count.head())
    

    # Calculate brokerage for each strategy per day
    trade_count["daily_brokerage"] = (
        trade_count["trade_count"] *
        trade_count["lots"] *
        trade_count["brokerage"]
    )
    print("--------------------trade_count with daily_brokerage---------------------------")
    print(trade_count.head().to_string(index=False))


    # Final brokerage per day (sum of all strategies)
    final_brokerage_per_day = (
        trade_count.groupby("date")["daily_brokerage"]
        .sum()
        .reset_index(name="final_brokerage_per_day")
    )
    print("---------------------final_brokerage_per_day after sum by date and daily_brokerage--------------------------")
    print(final_brokerage_per_day.head()) 

    # Calculating slippage
    # ---- 1. Add lots & slippage to df ----
    df["lots"] = df["strategy"].map(lots_map)
    df["slippage_rate"] = df["strategy"].map(slippage_map)

    print("------------------slippage table with lots and slippage_rate-----------------------------")
    print(df.head().to_string(index=False))

    # ---- 2. Slippage per trade ----
    df["slippage_per_trade"] = (
        (df["EntryPrice"] + df["ExitPrice"]) *
        df["lots"] *
        df["slippage_rate"]
    )
    # df.to_csv('slippage_per_trade.csv')

    # ---- 3. Slippage per day per strategy ----
    daily_slippage = (
        df.groupby(["date", "strategy"])["slippage_per_trade"]
        .sum()
        .reset_index(name="daily_slippage")
    )
    # first_date = daily_slippage["date"].min()
    print("------------------daily_slippage groupe by date and strategy-----------------------------")
    print(daily_slippage.head().to_string(index=False))


    # ---- 4. Total portfolio slippage per day ----
    final_slippage_per_day = (
        daily_slippage.groupby("date")["daily_slippage"]
        .sum()
        .reset_index(name="final_slippage_per_day")
    )

    daily_cost = pd.merge(
    final_brokerage_per_day,
    final_slippage_per_day,
    on="date",
    how="outer"
    )
    # first_date = daily_cost["date"].min()
    print("------------------Final_cost-----------------------------")
    print(daily_cost.head().to_string(index=False))

    daily_cost["final_cost"] = (
        daily_cost["final_brokerage_per_day"].fillna(0) +
        daily_cost["final_slippage_per_day"].fillna(0)
    )
    print("-----------------Final_cost after fillna(0)------------------------------")
    print(daily_cost.head().to_string(index=False))


    # 2. Fetch intraday MTM data (15-min frequency)
    cursor = db.strategies_mtm_data.find(
        {"strategy": {"$in": strategy_names}},
        {"Date": 1, "CumulativePnl": 1, "strategy": 1}
    ).batch_size(50000)

    docs = list(cursor)
    if not docs:
        return {"portfolio": portfolio_name, "ohlc": []}

    df = pd.DataFrame(docs)

    # Ensure Date is timezone-aware (UTC)
    df["Date"] = pd.to_datetime(df["Date"], utc=True)

    # Extract date for grouping
    df["date_only"] = df["Date"].dt.date

    # Apply lots multiplier
    df["scaled_pnl"] = df["CumulativePnl"] * df["strategy"].map(lots_map)

    # Pivot to align all strategies on same timestamps
    pivot = df.pivot_table(
        index="Date",
        columns="strategy",
        values="scaled_pnl",
        aggfunc="last"  # should be only one per strategy per timestamp
    ).ffill()  # forward fill in case some strategies don't update every 15 mins

    # Portfolio gross equity at each 15-min timestamp
    pivot["portfolio_equity_gross"] = pivot.sum(axis=1)

    # Prepare daily cost with proper date
    daily_cost["date"] = pd.to_datetime(daily_cost["date"]).dt.date

    # Create a map: date → final_cost
    cost_map = dict(zip(daily_cost["date"], daily_cost["final_cost"]))

    # Map daily cost to each 15-min row
    gross = pivot[["portfolio_equity_gross"]].reset_index()
    gross["date_only"] = gross["Date"].dt.date
    gross["daily_cost"] = gross["date_only"].map(cost_map).fillna(0)

    # Identify the LAST record of each day
    gross["is_eod"] = gross.groupby("date_only")["Date"].transform("max") == gross["Date"]

    # Deduct the entire day's cost ONLY at the last (EOD) timestamp
    gross["cost_deduction"] = 0.0
    gross.loc[gross["is_eod"], "cost_deduction"] = gross.loc[gross["is_eod"], "daily_cost"]

    # Cumulative cost up to this point (step function: jumps only at EOD)
    gross["cumulative_cost"] = gross["cost_deduction"]

    # Final: Net portfolio equity
    gross["portfolio_equity_net"] = gross["portfolio_equity_gross"] - gross["cumulative_cost"]

    # Build OHLC from net equity
    result = gross.sort_values("Date").copy()

    result["open"] = result["portfolio_equity_net"].shift(1)
    # First row: open = close
    result["open"] = result["open"].fillna(result["portfolio_equity_net"])

    result["close"] = result["portfolio_equity_net"]
    result["high"] = result[["open", "close"]].max(axis=1)
    result["low"]  = result[["open", "close"]].min(axis=1)

    # Convert to UNIX timestamp in IST (UTC → IST = -5:30 → subtract 19800 seconds)
    result["time"] = (result["Date"].astype("int64") // 10**9) - 19800

    # Final output
    ohlc = result[["time", "open", "high", "low", "close"]].to_dict("records")

    return ohlc

@router.get("/portfolio/{portfolio_name}/mtms")
async def get_portfolio_mtm(
    portfolio_name: str,
    db=Depends(get_db)
):
    """
        Build TRUE OHLC for portfolio using EVENT-DRIVEN CUMULATIVE PNL
        Same logic as strategy OHLC (Version 1)
    """

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

    # 6️⃣ Build OHLC 
    equity["open"] = equity["portfolio_equity"].shift(1).fillna(equity["portfolio_equity"])
    equity["close"] = equity["portfolio_equity"]

    equity["high"] = equity[["open", "close"]].max(axis=1)
    equity["low"]  = equity[["open", "close"]].min(axis=1)

    # 7️⃣ Convert UNIX time (IST)
    equity["time"] = ((equity["Date"].astype("int64") // 10**9) - 19800) * 1000

    out = equity[["time", "open", "high", "low", "close"]].to_dict("records")

    return out


@router.get("/portfolio/{portfolio_name}/mtm")
async def get_portfolio_mtm(
    portfolio_name: str,
    db=Depends(get_db)
):
    """
        Build TRUE OHLC for portfolio using EVENT-DRIVEN CUMULATIVE PNL
        Same logic as strategy OHLC (Version 1)
    """

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

    # 6️⃣ Build OHLC 
    equity["open"] = equity["portfolio_equity"].fillna(equity["portfolio_equity"])
    equity["close"] = equity["portfolio_equity"].shift(-1).fillna(equity["portfolio_equity"])

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