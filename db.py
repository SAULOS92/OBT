import os
import psycopg2

def conectar():
    """Devuelve una conexión a PostgreSQL (Neon)."""
    url = os.getenv("DATABASE_URL")
    return psycopg2.connect(url, sslmode="require")
