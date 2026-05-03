from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from contextlib import asynccontextmanager
from typing import List
from models import Product
import asyncio, json

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

async def consume(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            order = json.loads(msg.value.decode('utf-8'))
            product = products_db.get(order['product_id'])
            
            if product and product.quantity >= order['quantity']:
                product.quantity -= order['quantity']
                await producer.send_and_wait("order-confirmed", json.dumps({
                    "order_id": order['id'],
                    "product_id": product.id
                }).encode('utf-8'))
    except asyncio.CancelledError:
        pass

@app.get("/products")
def get_products():
    return products_db