from src.access import get_price_data

from typing import Dict, List

from fastapi import FastAPI


# ---------- API ---------- #
app =  FastAPI()


@app.get('/price_data/{symbol}')
async def get_price_data_route(symbol: str) -> List[Dict[str, float]]:
    return {'data': list(get_price_data(symbol))}