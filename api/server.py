"""
FastAPI server exposing the database as a REST API.
"""

from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Dict, List, Optional

from database.engine import DatabaseEngine

app = FastAPI(title="Simple RDBMS API", version="1.0.0")

# Enable CORS for web app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database engine
engine = DatabaseEngine("data")

# Pydantic models for request validation
class UserCreate(BaseModel):
    name: str
    email: str

class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None

class OrderCreate(BaseModel):
    user_id: int
    item: str
    amount: float

# ========== ENDPOINTS ==========

@app.get("/users")
async def get_users():
    """
    Get all users.
    """
    try:
        # First, check if users table exists
        tables = engine.list_tables()
        if "users" not in tables:
            return {"users": []}

        result = engine.execute("SELECT * FROM users")
        return {"users": result.get("rows", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/users")
async def create_user(user: UserCreate):
    """
    Create a new user.
    """
    try:
        # Check if users table exists, create if not
        tables = engine.list_tables()
        if "users" not in tables:
            # Create users table
            create_query = """
                CREATE TABLE users (
                    id INT PRIMARY KEY,
                    name TEXT,
                    email TEXT UNIQUE
                )
            """
            engine.execute(create_query)

        # Get next available ID
        users_result = engine.execute("SELECT * FROM users")
        users = users_result.get("rows", [])
        next_id = max([u.get("id", 0) for u in users], default=0) + 1

        # Insert user (using parameterized approach to avoid SQL injection)
        insert_query = f"INSERT INTO users VALUES ({next_id}, '{user.name}', '{user.email}')"
        result = engine.execute(insert_query)

        return {"status": "OK", "id": next_id, "message": "User created successfully"}
    except Exception as e:
        # Check if it's a unique constraint violation
        if "UNIQUE" in str(e).upper() or "DUPLICATE" in str(e).upper():
            raise HTTPException(status_code=400, detail=f"Email '{user.email}' already exists")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.put("/users/{user_id}")
async def update_user(user_id: int, user: UserUpdate):
    """
    Update an existing user.
    """
    try:
        # Check if user exists
        check_query = f"SELECT * FROM users WHERE id = {user_id}"
        check_result = engine.execute(check_query)
        if not check_result.get("rows"):
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Build update query
        updates = []
        if user.name is not None:
            updates.append(f"name = '{user.name}'")
        if user.email is not None:
            updates.append(f"email = '{user.email}'")

        if not updates:
            return {"status": "OK", "message": "No changes provided"}

        update_query = f"UPDATE users SET {', '.join(updates)} WHERE id = {user_id}"
        result = engine.execute(update_query)

        return {"status": "OK", "message": "User updated successfully"}
    except Exception as e:
        if "UNIQUE" in str(e).upper() or "DUPLICATE" in str(e).upper():
            raise HTTPException(status_code=400, detail="Email already exists")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.delete("/users/{user_id}")
async def delete_user(user_id: int):
    """
    Delete a user.
    """
    try:
        # Check if user exists
        check_query = f"SELECT * FROM users WHERE id = {user_id}"
        check_result = engine.execute(check_query)
        if not check_result.get("rows"):
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Check if user has orders
        tables = engine.list_tables()
        if "orders" in tables:
            orders_query = f"SELECT * FROM orders WHERE user_id = {user_id}"
            orders_result = engine.execute(orders_query)
            if orders_result.get("rows"):
                raise HTTPException(
                    status_code=400,
                    detail="Cannot delete user with existing orders. Delete orders first."
                )

        # Delete user
        delete_query = f"DELETE FROM users WHERE id = {user_id}"
        result = engine.execute(delete_query)

        return {"status": "OK", "message": "User deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/orders")
async def get_orders():
    """
    Get all orders.
    """
    try:
        # Check if orders table exists
        tables = engine.list_tables()
        if "orders" not in tables:
            return {"orders": []}

        result = engine.execute("SELECT * FROM orders")
        return {"orders": result.get("rows", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/orders")
async def create_order(order: OrderCreate):
    """
    Create a new order.
    """
    try:
        # Check if orders table exists, create if not
        tables = engine.list_tables()
        if "orders" not in tables:
            # Create orders table
            create_query = """
                CREATE TABLE orders (
                    id INT PRIMARY KEY,
                    user_id INT,
                    item TEXT,
                    amount DECIMAL
                )
            """
            engine.execute(create_query)

        # Check if user exists
        users_query = f"SELECT * FROM users WHERE id = {order.user_id}"
        users_result = engine.execute(users_query)
        if not users_result.get("rows"):
            raise HTTPException(status_code=400, detail=f"User with ID {order.user_id} does not exist")

        # Get next available ID
        orders_result = engine.execute("SELECT * FROM orders")
        orders = orders_result.get("rows", [])
        next_id = max([o.get("id", 0) for o in orders], default=0) + 1

        # Insert order
        insert_query = f"INSERT INTO orders VALUES ({next_id}, {order.user_id}, '{order.item}', {order.amount})"
        result = engine.execute(insert_query)

        return {"status": "OK", "id": next_id, "message": "Order created successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.delete("/orders/{order_id}")
async def delete_order(order_id: int):
    """
    Delete an order.
    """
    try:
        # Check if order exists
        check_query = f"SELECT * FROM orders WHERE id = {order_id}"
        check_result = engine.execute(check_query)
        if not check_result.get("rows"):
            raise HTTPException(status_code=404, detail=f"Order with ID {order_id} not found")

        # Delete order
        delete_query = f"DELETE FROM orders WHERE id = {order_id}"
        result = engine.execute(delete_query)

        return {"status": "OK", "message": "Order deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/user-orders")
async def get_user_orders():
    """
    Get users with their orders (INNER JOIN).
    """
    try:
        # Check if both tables exist
        tables = engine.list_tables()
        if "users" not in tables or "orders" not in tables:
            return {"data": []}

        # Execute JOIN query
        join_query = """
            SELECT users.id as user_id, users.name, users.email, 
                   orders.id as order_id, orders.item, orders.amount
            FROM users 
            INNER JOIN orders ON users.id = orders.user_id
            ORDER BY users.name, orders.id
        """
        result = engine.execute(join_query)

        return {"data": result.get("rows", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# ========== GENERAL PURPOSE ENDPOINTS ==========

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "Simple RDBMS API",
        "version": "1.0.0",
        "endpoints": {
            "demo": {
                "GET /users": "List all users",
                "POST /users": "Create new user",
                "PUT /users/{id}": "Update user",
                "DELETE /users/{id}": "Delete user",
                "GET /orders": "List all orders",
                "POST /orders": "Create new order",
                "DELETE /orders/{id}": "Delete order",
                "GET /user-orders": "Get joined user and order data"
            },
            "general": {
                "POST /query": "Execute SQL query",
                "GET /tables": "List all tables",
                "GET /tables/{name}": "Get table info"
            }
        }
    }

@app.get("/tables")
async def list_tables():
    """List all tables in the database."""
    try:
        tables = engine.list_tables()
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{table_name}")
async def get_table_info(table_name: str):
    """Get information about a specific table."""
    try:
        info = engine.get_table_info(table_name)
        return info
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def execute_query(query: Dict[str, Any] = Body(...)):
    """Execute a raw SQL-like query."""
    try:
        if "query" not in query:
            raise HTTPException(status_code=400, detail="Query string is required")

        result = engine.execute(query["query"])
        return result
    except (SyntaxError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)