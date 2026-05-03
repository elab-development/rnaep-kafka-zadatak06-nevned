from pydantic import BaseModel

class Notification(BaseModel):
    order_id: int
    product_id: int
    message: str