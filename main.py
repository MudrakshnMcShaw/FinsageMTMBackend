from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import strategy_ohlc, upload_file

app = FastAPI(
    title="FinSageAI MTM Strategy API",
    description="Backend for strategies and MTM OHLC data",
    version="1.0.0"
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

@app.get("/")
def home():
    return {"message": "FinSageAI MTM Strategy API is running."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
