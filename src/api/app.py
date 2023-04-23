from access import get_price_data

from fastapi import FastAPI


# ---------- API ---------- #
app =  FastAPI()


@app.get('/price_data')
async def get_price_data_route() -> dict:
    return {'data': get_price_data()}