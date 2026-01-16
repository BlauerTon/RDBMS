# Simple RDBMS - A Custom Relational Database Management System

## Project Overview
A fully functional, implementation of a relational database management system (RDBMS) built entirely in Python. This project demonstrates core database concepts through a modular architecture consisting of a database engine, REST API layer, and web application.

## Key Features
### 1. Database Engine
* SQL-like query language with parser and executor

* Support for INT, TEXT, BOOL, and DECIMAL data types

* PRIMARY KEY and UNIQUE constraints with automatic enforcement

* Basic hash-based indexing (automatic for primary/unique columns)

* INNER JOIN support with nested loop joins

* File-based persistence (JSON schemas + pickle data)

* Interactive command-line REPL interface

### 2. API Layer
* RESTful endpoints for CRUD operations

* FastAPI-based with automatic OpenAPI documentation

* Input validation using Pydantic models

* CORS-enabled for web application integration


### 3. Web Application
* Complete User & Orders Management System

* Demonstrates CRUD operations and JOIN queries

* Pure server-side rendering with Jinja2 templates

* Communicates exclusively through API layer

* Minimal, clean interface focused on functionality

## Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                    Web Application (Port 8080)              │
│    HTML/JS/CSS → FastAPI Server → HTTP Requests to API      │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    API Layer (Port 8000)                    │
│    FastAPI Server → Query Parser → Database Engine          │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    Database Engine                          │
│    Query Parser → Executor → Storage → Indexes              │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    Persistent Storage                       │
│                  JSON Schemas + Pickle Data                 │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start Guide
### Prerequisites
* Python 3.8 or higher
* Windows, macOS, or Linux

## Installation
### 1. Clone/Create the project structure

```
git clone https://github.com/BlauerTon/RDBMS.git
cd RDBMS
```

### 2.Create and activate virtual environment

```
bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies
```
pip install -r requirements.txt
```

### 4. Run the System
Open Terminal 1 - API Server:

```
python -m api.server
```
API will start at: http://localhost:8000

Open Terminal 2 - Web Application:
```
cd webapp
python app.py
```
Web app will start at: http://localhost:8080

### 5. Open Your Browser
Navigate to: http://localhost:8080

## Usage Examples

Using the Web Interface

* Create users with unique email addresses

* Create orders linked to users

* View joined user-order data

* Perform updates and deletions

## Using the Database REPL

```
# From project root:
python -m database.repl

# Try these commands:
db> tables                    # List all tables
db> SELECT * FROM users;      # View all users
db> help                      # Show available commands
```
### Testing the API
```
# List all users
curl http://localhost:8000/users

# Create a new user
curl -X POST http://localhost:8000/users \
  -H "Content-Type: application/json" \
  -d '{"name":"John Doe","email":"john@example.com"}'
```
