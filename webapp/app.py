"""
Minimal web application for the RDBMS.
"""

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import httpx
from typing import Dict, Any, List, Optional
import os

# Create FastAPI app
app = FastAPI(title="Simple RDBMS Web App")

# Get absolute paths for static files and templates
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

print(f"Base directory: {BASE_DIR}")
print(f"Static directory: {STATIC_DIR}")
print(f"Templates directory: {TEMPLATES_DIR}")

# Verify directories exist
if not os.path.exists(STATIC_DIR):
    print(f"Creating static directory: {STATIC_DIR}")
    os.makedirs(STATIC_DIR, exist_ok=True)

if not os.path.exists(TEMPLATES_DIR):
    print(f"Creating templates directory: {TEMPLATES_DIR}")
    os.makedirs(TEMPLATES_DIR, exist_ok=True)

# Mount static files - THIS IS CRITICAL
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Setup templates
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# API configuration
API_URL = "http://localhost:8000"

class APIClient:
    """Client to communicate with the RDBMS API."""

    def __init__(self, base_url: str = API_URL):
        self.base_url = base_url

    async def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request to API."""
        url = f"{self.base_url}{endpoint}"

        async with httpx.AsyncClient() as client:
            response = await client.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()

    async def get_users(self) -> List[Dict[str, Any]]:
        """Get all users."""
        result = await self._request("GET", "/users")
        return result.get("users", [])

    async def create_user(self, name: str, email: str) -> Dict[str, Any]:
        """Create a new user."""
        return await self._request("POST", "/users", json={
            "name": name,
            "email": email
        })

    async def update_user(self, user_id: int, name: str, email: str) -> Dict[str, Any]:
        """Update an existing user."""
        return await self._request("PUT", f"/users/{user_id}", json={
            "name": name,
            "email": email
        })

    async def delete_user(self, user_id: int) -> Dict[str, Any]:
        """Delete a user."""
        return await self._request("DELETE", f"/users/{user_id}")

    async def get_orders(self) -> List[Dict[str, Any]]:
        """Get all orders."""
        result = await self._request("GET", "/orders")
        return result.get("orders", [])

    async def create_order(self, user_id: int, item: str, amount: float) -> Dict[str, Any]:
        """Create a new order."""
        return await self._request("POST", "/orders", json={
            "user_id": user_id,
            "item": item,
            "amount": amount
        })

    async def delete_order(self, order_id: int) -> Dict[str, Any]:
        """Delete an order."""
        return await self._request("DELETE", f"/orders/{order_id}")

    async def get_user_orders(self) -> List[Dict[str, Any]]:
        """Get users with their orders (INNER JOIN)."""
        result = await self._request("GET", "/user-orders")
        return result.get("data", [])

client = APIClient()

# Debug endpoint to check static files
@app.get("/debug")
async def debug():
    """Debug endpoint to check paths and files."""
    css_path = os.path.join(STATIC_DIR, "style.css")

    return {
        "base_dir": BASE_DIR,
        "static_dir": STATIC_DIR,
        "templates_dir": TEMPLATES_DIR,
        "css_file_exists": os.path.exists(css_path),
        "css_file_path": css_path,
        "current_working_dir": os.getcwd(),
        "files_in_static": os.listdir(STATIC_DIR) if os.path.exists(STATIC_DIR) else []
    }

# Routes
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Home page."""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/users", response_class=HTMLResponse)
async def list_users(request: Request):
    """Display all users."""
    try:
        users = await client.get_users()
        return templates.TemplateResponse(
            "users.html",
            {"request": request, "users": users, "error": None}
        )
    except Exception as e:
        return templates.TemplateResponse(
            "users.html",
            {"request": request, "users": [], "error": str(e)}
        )

@app.get("/users/new", response_class=HTMLResponse)
async def new_user_form(request: Request):
    """Form to create new user."""
    return templates.TemplateResponse(
        "user_form.html",
        {"request": request, "user": None, "action": "Create", "error": None}
    )

@app.post("/users/new")
async def create_user(
    request: Request,
    name: str = Form(...),
    email: str = Form(...)
):
    """Handle new user creation."""
    try:
        await client.create_user(name, email)
        return RedirectResponse("/users", status_code=303)
    except Exception as e:
        return templates.TemplateResponse(
            "user_form.html",
            {
                "request": request,
                "user": {"name": name, "email": email},
                "action": "Create",
                "error": str(e)
            }
        )

@app.get("/users/{user_id}/edit", response_class=HTMLResponse)
async def edit_user_form(request: Request, user_id: int):
    """Form to edit user."""
    try:
        users = await client.get_users()
        user = next((u for u in users if u["id"] == user_id), None)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        return templates.TemplateResponse(
            "user_form.html",
            {"request": request, "user": user, "action": "Update", "error": None}
        )
    except Exception as e:
        return RedirectResponse(f"/users?error={str(e)}", status_code=303)

@app.post("/users/{user_id}/edit")
async def update_user(
    request: Request,
    user_id: int,
    name: str = Form(...),
    email: str = Form(...)
):
    """Handle user update."""
    try:
        await client.update_user(user_id, name, email)
        return RedirectResponse("/users", status_code=303)
    except Exception as e:
        users = await client.get_users()
        user = next((u for u in users if u["id"] == user_id), None)

        return templates.TemplateResponse(
            "user_form.html",
            {
                "request": request,
                "user": {"id": user_id, "name": name, "email": email},
                "action": "Update",
                "error": str(e)
            }
        )

@app.post("/users/{user_id}/delete")
async def delete_user(request: Request, user_id: int):
    """Handle user deletion."""
    try:
        await client.delete_user(user_id)
        return RedirectResponse("/users", status_code=303)
    except Exception as e:
        return RedirectResponse(f"/users?error={str(e)}", status_code=303)

@app.get("/orders", response_class=HTMLResponse)
async def list_orders(request: Request):
    """Display all orders."""
    try:
        orders = await client.get_orders()
        users = await client.get_users()

        # Add user names to orders
        user_map = {u["id"]: u["name"] for u in users}
        for order in orders:
            order["user_name"] = user_map.get(order["user_id"], "Unknown")

        return templates.TemplateResponse(
            "orders.html",
            {"request": request, "orders": orders, "users": users, "error": None}
        )
    except Exception as e:
        return templates.TemplateResponse(
            "orders.html",
            {"request": request, "orders": [], "users": [], "error": str(e)}
        )

@app.get("/orders/new", response_class=HTMLResponse)
async def new_order_form(request: Request):
    """Form to create new order."""
    try:
        users = await client.get_users()
        return templates.TemplateResponse(
            "order_form.html",
            {"request": request, "users": users, "error": None}
        )
    except Exception as e:
        return RedirectResponse(f"/orders?error={str(e)}", status_code=303)

@app.post("/orders/new")
async def create_order(
    request: Request,
    user_id: int = Form(...),
    item: str = Form(...),
    amount: float = Form(...)
):
    """Handle new order creation."""
    try:
        await client.create_order(user_id, item, amount)
        return RedirectResponse("/orders", status_code=303)
    except Exception as e:
        users = await client.get_users()
        return templates.TemplateResponse(
            "order_form.html",
            {
                "request": request,
                "users": users,
                "form_data": {"user_id": user_id, "item": item, "amount": amount},
                "error": str(e)
            }
        )

@app.post("/orders/{order_id}/delete")
async def delete_order(request: Request, order_id: int):
    """Handle order deletion."""
    try:
        await client.delete_order(order_id)
        return RedirectResponse("/orders", status_code=303)
    except Exception as e:
        return RedirectResponse(f"/orders?error={str(e)}", status_code=303)

@app.get("/user-orders", response_class=HTMLResponse)
async def user_orders_view(request: Request):
    """Display users with orders (JOIN demonstration)."""
    try:
        data = await client.get_user_orders()
        return templates.TemplateResponse(
            "user_orders.html",
            {"request": request, "user_orders": data, "error": None}
        )
    except Exception as e:
        return templates.TemplateResponse(
            "user_orders.html",
            {"request": request, "user_orders": [], "error": str(e)}
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)