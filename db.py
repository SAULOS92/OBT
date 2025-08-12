import os
import time
from psycopg_pool import ConnectionPool
from psycopg import OperationalError

# URL de conexión desde variable de entorno o fija
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://usuario:password@host/dbname")

# Configuración del pool optimizada para Neon free tier
pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=0,       # No mantiene conexiones vivas si no hay uso
    max_size=12,      # Hasta 12 conexiones concurrentes
    max_idle=30,      # Cierra conexiones inactivas después de 30s
    timeout=10,       # Espera máximo 10s por conexión libre
    num_workers=3     # Trabajadores internos del pool
)

def conectar(reintentos=3, espera=2):
    """
    Devuelve una conexión del pool lista para usar con:
        conn = conectar()
        cur = conn.cursor()
    Si la base está dormida, reintenta automáticamente.
    """
    for intento in range(1, reintentos + 1):
        try:
            # ConnectionPool.connection() returns a context manager, which
            # doesn't expose the cursor() method directly.  Use getconn()
            # instead to obtain an actual connection object from the pool.
            conn = pool.getconn()
            return conn
        except OperationalError as e:
            if intento < reintentos:
                print(f"[DB] Conexión fallida (intento {intento}/{reintentos}). Reintentando en {espera}s...")
                time.sleep(espera)
            else:
                print("[DB] No se pudo conectar a la base de datos.")
                raise e

