"""Operaciones de base de datos para rutas y placas de vehículos."""

from db import conectar

PLACA_MAX_LEN = 17


def ensure_table() -> None:
    """Crea la tabla `vehiculos` si aún no existe."""

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS vehiculos (
                    bd   TEXT NOT NULL,
                    ruta INTEGER NOT NULL,
                    placa VARCHAR({PLACA_MAX_LEN}) NOT NULL DEFAULT '',
                    PRIMARY KEY (bd, ruta)
                );
                """
            )
            cur.execute(
                """
                SELECT character_maximum_length
                FROM information_schema.columns
                WHERE table_name='vehiculos' AND column_name='placa' AND table_schema='public'
                """
            )
            current_len = cur.fetchone()
            current_len = current_len[0] if current_len else None

            if current_len is None or current_len < PLACA_MAX_LEN:
                cur.execute(
                    f"ALTER TABLE vehiculos ALTER COLUMN placa TYPE VARCHAR({PLACA_MAX_LEN});"
                )
            conn.commit()


def get_vehiculos(bd: str):
    """Devuelve todas las rutas y placas registradas para una empresa."""

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ruta, placa FROM vehiculos WHERE bd=%s ORDER BY ruta", (bd,))
            rows = cur.fetchall()
            if not rows:
                cur.execute(
                    "INSERT INTO vehiculos (bd, ruta, placa) VALUES (%s, %s, %s)",
                    (bd, 1, ""),
                )
                conn.commit()
                rows = [(1, "")]
    return [{"ruta": r, "placa": p} for r, p in rows]


def upsert_vehiculo(bd: str, ruta: int, placa: str) -> None:
    """Inserta o actualiza una placa para la ruta indicada."""

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO vehiculos (bd, ruta, placa)
                VALUES (%s, %s, %s)
                ON CONFLICT (bd, ruta) DO UPDATE SET placa = EXCLUDED.placa
                """,
                (bd, ruta, placa[:PLACA_MAX_LEN]),
            )
            conn.commit()


def add_ruta(bd: str):
    """Crea una nueva ruta vacía y la devuelve."""

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(ruta),0)+1 FROM vehiculos WHERE bd=%s", (bd,))
            next_ruta = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO vehiculos (bd, ruta, placa) VALUES (%s, %s, %s)",
                (bd, next_ruta, ""),
            )
            conn.commit()
    return {"ruta": next_ruta, "placa": ""}


def delete_ruta(bd: str, ruta: int) -> bool:
    """Elimina la ruta indicada. Devuelve True si alguna fila fue borrada."""

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM vehiculos WHERE bd=%s AND ruta=%s", (bd, ruta))
            deleted = cur.rowcount > 0
            conn.commit()
    return deleted
