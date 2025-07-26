from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from database import engine, get_db
import models
import datetime

app = FastAPI()

class ChatRequest(BaseModel):
    user_id: int
    message: str

@app.get("/")
def read_root():
    return {"message": "Welcome to the E-commerce API"}

# Create tables if they don't exist
models.Base.metadata.create_all(bind=engine)

@app.post("/chat")
def chat_endpoint(request: ChatRequest, db: Session = Depends(get_db)):
    user_id = request.user_id
    message = request.message
    response_text = None
    data = None
    try:
        msg_lower = message.lower()
        # Top products intent
        if "top products" in msg_lower or "best sellers" in msg_lower:
            top_products = (
                db.query(models.Product)
                .order_by(models.Product.quantity_sold.desc())
                .limit(5)
                .all()
            )
            if top_products:
                names = [p.name for p in top_products]
                response_text = f"Top 5 products are: {', '.join(names)}."
                data = [
                    {"name": p.name, "category": p.category, "price": p.price, "quantity_sold": p.quantity_sold}
                    for p in top_products
                ]
            else:
                response_text = "No products found."
                data = []
        # Latest order intent
        elif "order" in msg_lower or "my order" in msg_lower:
            latest_order = (
                db.query(models.Order)
                .filter(models.Order.user_id == user_id)
                .order_by(models.Order.order_date.desc())
                .first()
            )
            if latest_order:
                response_text = (
                    f"Your latest order (ID: {latest_order.id}) was placed on {latest_order.order_date.strftime('%Y-%m-%d %H:%M:%S')} with status '{latest_order.status.value}'."
                )
                data = {
                    "id": latest_order.id,
                    "order_date": latest_order.order_date,
                    "status": latest_order.status.value,
                    "product_id": latest_order.product_id
                }
            else:
                response_text = "You have no orders yet."
                data = None
        # Product name intent
        else:
            # Try to match a product name
            products = db.query(models.Product).all()
            found_product = None
            for product in products:
                if product.name.lower() in msg_lower:
                    found_product = product
                    break
            if found_product:
                inventory = db.query(models.Inventory).filter(models.Inventory.product_id == found_product.id).first()
                qty = inventory.quantity_available if inventory else 'unknown'
                response_text = f"Product '{found_product.name}': Price ${found_product.price}, Available: {qty}"
                data = {
                    "name": found_product.name,
                    "category": found_product.category,
                    "price": found_product.price,
                    "quantity_available": qty
                }
            else:
                response_text = "Sorry, I couldn't understand that."
                data = None
        # Log the conversation
        conv = models.Conversation(
            user_id=user_id,
            message=message,
            response=response_text,
            timestamp=datetime.datetime.utcnow()
        )
        db.add(conv)
        db.commit()
        db.refresh(conv)
        return {"response": response_text, "data": data}
    except Exception as e:
        db.rollback()
        # Log the failed conversation
        conv = models.Conversation(
            user_id=user_id,
            message=message,
            response="Sorry, I couldn't understand that.",
            timestamp=datetime.datetime.utcnow()
        )
        db.add(conv)
        db.commit()
        db.refresh(conv)
        return {"response": "Sorry, I couldn't understand that.", "data": None}

@app.get("/chat/history/{user_id}")
def chat_history(user_id: int, db: Session = Depends(get_db)):
    messages = (
        db.query(models.Conversation)
        .filter(models.Conversation.user_id == user_id)
        .order_by(models.Conversation.timestamp.desc())
        .limit(20)
        .all()
    )
    return [
        {
            "id": msg.id,
            "message": msg.message,
            "sender": msg.sender.value,
            "timestamp": msg.timestamp
        }
        for msg in messages
    ] 