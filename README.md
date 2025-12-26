# Arcanum

**arcanum** is a Python library designed to seamlessly bind Pydantic schemas with various datasources, eliminating the need to manually create templates, factories, and utilities repeatedly. It provides a unified interface for working with different data backends while maintaining type safety and validation through Pydantic.

## 🎯 Purpose

Modern applications often require integration with multiple data sources (databases, APIs, file systems, etc.), and developers frequently find themselves writing similar boilerplate code for each integration. arcanum addresses this by:

- **Automatic Schema Binding**: Automatically generates database models, API clients, and data access patterns from Pydantic schemas
- **Type Safety**: Leverages Pydantic's validation and type hints for runtime safety
- **Multiple Backend Support**: Works with PostgreSQL, SQLite, MongoDB, and more
- **Reduced Boilerplate**: Eliminates repetitive code for CRUD operations, serialization, and validation
- **Development Efficiency**: Faster prototyping and development with consistent patterns

## 🚀 Features

- **Schema-First Development**: Define your data models once with Pydantic, use everywhere
- **Database Integration**: Automatic SQLAlchemy model generation and migration support
- **API Generation**: Auto-generate FastAPI endpoints from schemas
- **Validation Pipeline**: Built-in data validation and transformation
- **Multiple Datasources**: Support for SQL databases, NoSQL, REST APIs, and file formats
- **Development Tools**: Docker-based development environment with PostgreSQL

## 📋 Requirements

- Python 3.9+
- UV package manager (recommended) or pip

## 🛠️ Installation

### Using UV (Recommended)

```bash
# Clone the repository
git clone https://github.com/kalynnka/arcanum.git
cd arcanum

# Install UV if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Install with development dependencies
uv sync --dev
```

### Using pip

```bash
# Install from source
pip install -e .

# Or install with development dependencies
pip install -e ".[dev]"
```

## 🐳 Development Environment

arcanum includes a Docker Compose setup for local development with PostgreSQL:

### Start the Development Database

```bash
# Start PostgreSQL
docker-compose up -d postgres

# Start with pgAdmin (optional)
docker-compose --profile admin up -d
```

### Database Connection Details

- **Host**: localhost
- **Port**: 5432
- **Database**: arcanum_dev
- **Username**: arcanum
- **Password**: dev_password

### pgAdmin Access (Optional)

If you started with the admin profile:
- **URL**: http://localhost:8080
- **Email**: admin@arcanum.local
- **Password**: admin_password

### Stop Services

```bash
docker-compose down
```

## 🏗️ Project Structure

```
arcanum/
├── arcanum/           # Main package
├── tests/                 # Test suite
├── docker/               # Docker configuration
│   └── init.sql          # Database initialization
├── docker-compose.yml    # Development services
├── pyproject.toml        # Project configuration
└── README.md
```

## 🔧 Development Workflow

1. **Start the development environment**:
   ```bash
   docker-compose up -d postgres
   ```

2. **Install dependencies**:
   ```bash
   uv sync --dev
   ```

3. **Run tests**:
   ```bash
   uv run pytest
   ```

4. **Code formatting**:
   ```bash
   uv run black arcanum/
   uv run isort arcanum/
   ```

5. **Type checking**:
   ```bash
   uv run mypy arcanum/
   ```

## 📖 Quick Start

```python
from pydantic import BaseModel
from arcanum import DataSourceBinding

# Define your schema
class User(BaseModel):
    id: int
    name: str
    email: str

# Create a binding
binding = DataSourceBinding(User)

# Automatic database integration
user_model = binding.to_sqlalchemy()  # SQLAlchemy model
user_table = binding.to_table()       # Direct table operations

# API integration
user_router = binding.to_fastapi()    # FastAPI router

# Data validation and transformation
validated_user = binding.validate(raw_data)
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests and linting
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Links

- **Repository**: https://github.com/kalynnka/arcanum
- **Issues**: https://github.com/kalynnka/arcanum/issues
- **Documentation**: Coming soon

## 🙏 Acknowledgments

- [Pydantic](https://pydantic.dev/) for excellent data validation
- [SQLAlchemy](https://sqlalchemy.org/) for database abstraction
- [FastAPI](https://fastapi.tiangolo.com/) for modern API development
