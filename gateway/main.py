from fastapi import FastAPI
import requests
from fastapi.responses import JSONResponse

app = FastAPI(title="API Gateway")

PRODUCTS_URL = "http://products-service:8000/products"
ORDERS_URL = "http://orders-service:8000/orders"
NOTIFICATIONS_URL = "http://notifications-service:8000/notifications"

@app.get("/products")
def get_products():
    response = requests.get(PRODUCTS_URL)
    return JSONResponse(content=response.json())

@app.get("/orders")
def get_orders():
    response = requests.get(ORDERS_URL)
    return JSONResponse(content=response.json())

@app.post("/orders")
def create_order(order: dict):
    response = requests.post(ORDERS_URL, json=order)
    return JSONResponse(content=response.json())

@app.get("/notifications")
def get_notifications():
    response = requests.get(NOTIFICATIONS_URL)
    return JSONResponse(content=response.json())