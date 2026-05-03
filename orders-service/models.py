from pydantic import BaseModel

class Order(BaseModel): 
    id: int 
    product_id: int 
    quantity: int 