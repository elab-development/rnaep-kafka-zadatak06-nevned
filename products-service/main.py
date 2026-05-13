from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from contextlib import asynccontextmanager
from typing import List
from models import Product
import asyncio
import json
from datetime import datetime

producer = AIOKafkaProducer(bootstrap_servers='kafka:9092')

@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    consumer = AIOKafkaConsumer(
        "order-created",
        bootstrap_servers='kafka:9092',
        group_id="products-group",
        auto_offset_reset="earliest"
    )
    await consumer.start()
    task = asyncio.create_task(consume(consumer))
    
    yield
    
    task.cancel()
    await consumer.stop()
    await producer.stop()

app = FastAPI(title="Products Service", lifespan=lifespan)

products_db = {
    1: Product(id=1, name="Laptop", price=1500.0, quantity=10),
    2: Product(id=2, name="Mouse", price=25.0, quantity=50)
}

async def send_error_event(topic: str, order_id: int, product_id: int, reason: str):
    payload = {
        "order_id": order_id,
        "product_id": product_id,
        "timestamp": datetime.utcnow().isoformat(),
        "error_reason": reason
    }
    await producer.send_and_wait(topic, json.dumps(payload).encode('utf-8'))

async def consume(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            order = json.loads(msg.value.decode('utf-8'))
            order_id = order.get('id')
            product_id = order.get('product_id')
            requested_qty = order.get('quantity')
            
            product = products_db.get(product_id)
            
            
            if not product:
                await send_error_event(
                    "product_not_found_events", 
                    order_id, 
                    product_id, 
                    "Proizvod ne postoji u katalogu"
                )
                continue

            
            if product.quantity < requested_qty:
                await send_error_event(
                    "out_of_stock_events", 
                    order_id, 
                    product_id, 
                    "Nedovoljna kolicina na stanju"
                )
                continue

           
            product.quantity -= requested_qty
            await producer.send_and_wait("order-confirmed", json.dumps({
                "order_id": order_id,
                "product_id": product.id
            }).encode('utf-8'))

    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"Error in consumer: {e}")

@app.get("/products")
def get_products():
    return products_db