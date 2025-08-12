import os
import time
from psycopg_pool import ConnectionPool
from psycopg import OperationalError

# URL de conexión desde variable de entorno
DATABASE_URL = os.getenv("DATABASE_URL")

# Configuración del pool optimizada para Neon Free Tier
pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,        # Mantiene siempre 1 conexión activa
    max_size=12,       # Hasta 12 conexiones simultáneas
    max_idle=30,       # Cierra conexiones inactivas después de 30s
    timeout=30,        # Espera máx. 30s por una conexión libre
    num_workers=3
)

def conectar(reintentos=3, espera=2):
    """
    Obtiene una conexión del pool.
    Uso:
        conn = conectar()
        cur = conn.cursor()
        ...
        liberar(conn)
    """
    for intento in range(1, reintentos + 1):
        try:
            t0 = time.time()
            conn = pool.getconn()
            print(f"[DB] Conexión obtenida en {time.time()-t0:.2f}s")
            return conn
        except OperationalError as e:
            if intento < reintentos:
                print(f"[DB] Conexión fallida (intento {intento}/{reintentos}). Reintentando en {espera}s...")
                time.sleep(espera)
            else:
                print("[DB] No se pudo conectar a la base de datos.")
                raise e

def liberar(conn):
    """
    Devuelve una conexión al pool.
    Siempre llamar en un bloque finally:
        conn = None
        try:
            conn = conectar()
            ...
        finally:
            liberar(conn)
    """
    if conn:
        pool.putconn(conn)
        print("[DB] Conexión liberada al pool")


