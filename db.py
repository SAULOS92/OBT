import os
from psycopg import connect


DATABASE_URL = os.getenv("DATABASE_URL")

def conectar():    
    return connect(DATABASE_URL)

