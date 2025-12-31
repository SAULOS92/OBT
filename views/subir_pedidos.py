"""Gestión simple de rutas y captura de credenciales para subir pedidos."""

from typing import Dict, Optional, Tuple
import threading

from flask import Blueprint, render_template, request, session, jsonify

from db import conectar
from views.auth import login_required

subir_pedidos_bp = Blueprint("subir_pedidos", __name__, template_folder="../templates")

# Estados básicos por ruta
_states_lock = threading.Lock()
_job_states: Dict[Tuple[str, int], Dict[str, Optional[str]]] = {}
_credentials: Dict[Tuple[str, int], Dict[str, str]] = {}


# ---------------------------------------------------------------------------
# Helpers BD
# ---------------------------------------------------------------------------


def _ensure_table() -> None:
    """Crea la tabla vehiculos si no existe."""

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS vehiculos (
                    bd   TEXT NOT NULL,
                    ruta INTEGER NOT NULL,
                    placa VARCHAR(10) NOT NULL DEFAULT '',
                    PRIMARY KEY (bd, ruta)
                );
                """
            )
            conn.commit()


def _get_bd() -> str:
    """Obtiene de la sesión la base de datos seleccionada."""

    bd = session.get("empresa")
    if not bd:
        raise ValueError("Falta empresa en sesión")
    return bd


def _get_vehiculos(bd: str):
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
    vehiculos = []
    for r in rows:
        with _states_lock:
            st = _job_states.get((bd, r[0]), {})
        vehiculos.append(
            {
                "ruta": r[0],
                "placa": r[1],
                "estado": st.get("status", "pendiente"),
                "paso": None,
            }
        )
    return vehiculos


def _upsert_vehiculo(bd: str, ruta: int, placa: str) -> None:
    """Inserta o actualiza una placa para la ruta indicada."""

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO vehiculos (bd, ruta, placa)
                VALUES (%s, %s, %s)
                ON CONFLICT (bd, ruta) DO UPDATE SET placa = EXCLUDED.placa
                """,
                (bd, ruta, placa[:10]),
            )
            conn.commit()


def _add_ruta(bd: str) -> Dict[str, int]:
    """Agrega una nueva ruta vacía para la empresa dada."""

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(ruta),0)+1 FROM vehiculos WHERE bd=%s", (bd,))
            next_ruta = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO vehiculos (bd, ruta, placa) VALUES (%s, %s, %s)",
                (bd, next_ruta, ""),
            )
            conn.commit()
    return {"ruta": next_ruta, "placa": "", "estado": "pendiente", "paso": None}


# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------


@subir_pedidos_bp.route("/subir-pedidos", methods=["GET"])
@login_required
def subir_pedidos_index():
    """Muestra la pantalla principal para gestionar rutas y credenciales."""

    _ensure_table()
    bd = _get_bd()
    vehiculos = _get_vehiculos(bd)
    return render_template("subir_pedidos.html", vehiculos=vehiculos, bd=bd)


@subir_pedidos_bp.route("/vehiculos/placa", methods=["POST"])
@login_required
def guardar_placa():
    """Guarda la placa asociada a una ruta enviada desde el formulario."""

    try:
        data = request.get_json() or {}
        bd = _get_bd()
        ruta = int(data.get("ruta"))
        placa = data.get("placa", "")
        _upsert_vehiculo(bd, ruta, placa)
        return jsonify(success=True, data={"ruta": ruta, "placa": placa})
    except Exception as e:
        return jsonify(success=False, error=str(e)), 400


@subir_pedidos_bp.route("/vehiculos/add", methods=["POST"])
@login_required
def agregar_ruta():
    """Crea una nueva ruta vacía y la devuelve al cliente."""

    try:
        bd = _get_bd()
        nuevo = _add_ruta(bd)
        return jsonify(success=True, data=nuevo)
    except Exception as e:
        return jsonify(success=False, error=str(e)), 400


@subir_pedidos_bp.route("/vehiculos/play", methods=["POST"])
@login_required
def ejecutar_ruta():
    """Captura las credenciales enviadas desde el formulario."""

    try:
        data = request.get_json() or {}
        bd = _get_bd()
        ruta = int(data.get("ruta"))
        usuario = data.get("usuario")
        password = data.get("contrasena")
        if not usuario or not password:
            return jsonify(success=False, error="Credenciales requeridas"), 400
        with _states_lock:
            _credentials[(bd, ruta)] = {"usuario": usuario, "contrasena": password}
            _job_states[(bd, ruta)] = {"status": "credenciales_recibidas", "paso_actual": None}
        return jsonify(success=True, data={"status": "credenciales_recibidas"})
    except Exception as e:
        return jsonify(success=False, error=str(e)), 400


@subir_pedidos_bp.route("/vehiculos/stop", methods=["POST"])
@login_required
def detener_ruta():
    """Limpia las credenciales y marca la ruta como cancelada."""

    try:
        data = request.get_json() or {}
        bd = _get_bd()
        ruta = int(data.get("ruta"))
        with _states_lock:
            _credentials.pop((bd, ruta), None)
            _job_states[(bd, ruta)] = {"status": "cancelado", "paso_actual": None}
            status = _job_states[(bd, ruta)]["status"]
        return jsonify(success=True, data={"status": status})
    except Exception as e:
        return jsonify(success=False, error=str(e)), 400


@subir_pedidos_bp.route("/vehiculos/estado", methods=["GET"])
@login_required
def estado_ruta():
    """Devuelve el estado actual de una ruta."""

    bd = request.args.get("bd") or _get_bd()
    ruta = int(request.args.get("ruta", 0))
    with _states_lock:
        st = _job_states.get((bd, ruta), {"status": "pendiente", "paso_actual": None})
    return jsonify(
        success=True,
        data={
            "status": st.get("status", "pendiente"),
            "paso": st.get("paso_actual"),
            "failed_step": st.get("failed_step"),
        },
    )
