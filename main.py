from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from routes import strategy_ohlc, upload_file, portfolio_ohlc, chart_layout, renko_ohlc
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

app = FastAPI(
    title="FinSageAI MTM Strategy API",
    description="Backend for strategies and MTM OHLC data",
    version="1.0.0"
)

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )
# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# include router
app.include_router(strategy_ohlc.router)
app.include_router(upload_file.router)
app.include_router(portfolio_ohlc.router)
app.include_router(chart_layout.router)
app.include_router(renko_ohlc.router)

@app.get("/")
def home():
    return {"message": "FinSageAI MTM Strategy API is running."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
