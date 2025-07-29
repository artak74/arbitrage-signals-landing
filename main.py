from fastapi import FastAPI
import os

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Arbitrage Signals API is running!"}

@app.post("/webhooks/nowpayments")
def nowpayments_webhook():
    return {"status": "webhook received"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
