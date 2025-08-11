import os
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, Tuple

import pandas as pd
from flask import (
    Blueprint,
    render_template,
    request,
    session,
    jsonify,
)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from db import conectar
from views.auth import login_required

# ---------------------------------------------------------------------------
# Configuración global: executor y estados de jobs
# ---------------------------------------------------------------------------
subir_pedidos_bp = Blueprint("subir_pedidos", __name__, template_folder="../templates")

_executor = ThreadPoolExecutor(max_workers=3)
_states_lock = threading.Lock()
_job_states: Dict[Tuple[str, int], Dict[str, str]] = {}

# ---------------------------------------------------------------------------
# Helpers BD
# ---------------------------------------------------------------------------

def _ensure_table() -> None:
    """Crea la tabla vehiculos si no existe."""
    conn = conectar()
    cur = conn.cursor()
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
    cur.close()
    conn.close()


def _get_bd() -> str:
    bd = session.get("empresa")
    if not bd:
        raise ValueError("Falta empresa en sesión")
    return bd


def _get_vehiculos(bd: str):
    conn = conectar()
    cur = conn.cursor()
    cur.execute("SELECT ruta, placa FROM vehiculos WHERE bd=%s ORDER BY ruta", (bd,))
    rows = cur.fetchall()
    if not rows:
        cur.execute(
            "INSERT INTO vehiculos (bd, ruta, placa) VALUES (%s, %s, %s)",
            (bd, 1, ""),
        )
        conn.commit()
        rows = [(1, "")]
    cur.close()
    conn.close()
    vehiculos = []
    for r in rows:
        with _states_lock:
            st = _job_states.get((bd, r[0]), {})
        vehiculos.append(
            {"ruta": r[0], "placa": r[1], "estado": st.get("status", "pendiente")}
        )
    return vehiculos


def _upsert_vehiculo(bd: str, ruta: int, placa: str) -> None:
    conn = conectar()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO vehiculos (bd, ruta, placa)
        VALUES (%s, %s, %s)
        ON CONFLICT (bd, ruta) DO UPDATE SET placa = EXCLUDED.placa
        """,
        (bd, ruta, placa[:10]),
    )
    conn.commit()
    cur.close()
    conn.close()


def _add_ruta(bd: str) -> Dict[str, int]:
    conn = conectar()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(ruta),0)+1 FROM vehiculos WHERE bd=%s", (bd,))
    next_ruta = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO vehiculos (bd, ruta, placa) VALUES (%s, %s, %s)",
        (bd, next_ruta, ""),
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"ruta": next_ruta, "placa": "", "estado": "pendiente"}

# ---------------------------------------------------------------------------
# Lógica Selenium reutilizada
# ---------------------------------------------------------------------------

def subir_pedidos_ruta(bd: str, ruta: int, usuario: str, password: str) -> None:
    conn = conectar()
    cur = conn.cursor()
    cur.execute("SELECT fn_obtener_pedidos_con_pedir_json(%s);", (bd,))
    pedidos = json.loads(cur.fetchone()[0])
    cur.close()
    conn.close()

    registros = [p for p in pedidos if p["ruta"] == ruta]
    if not registros:
        raise ValueError(f"Sin pedidos para ruta {ruta}")

    df = pd.DataFrame(registros)[["codigo_pro", "producto", "pedir"]]
    df.insert(2, "UN", "UN")
    fichero = f"{ruta}.xlsx"
    df.to_excel(
        fichero,
        sheet_name="Pedidos",
        startrow=4,
        index=False,
        engine="openpyxl",
    )

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)

    driver.get("https://portal.gruponutresa.com/")
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.NAME, "username"))
    ).send_keys(usuario)
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.NAME, "password"))
    ).send_keys(password)
    driver.find_element(By.CSS_SELECTOR, "form").submit()

    driver.get("https://portal.gruponutresa.com/p/nuevo/pedido-masivo/excel")
    file_input = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="file"]'))
    )
    file_input.send_keys(os.path.abspath(fichero))
    driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()

    try:
        btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Agregar al carrito')]") )
        )
        btn.click()
    except Exception:
        pass

    driver.get("https://portal.gruponutresa.com/carrito/resumen")
    try:
        WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(., 'Confirmar pedido')]")
            )
        ).click()
    except Exception:
        pass

    driver.quit()
    os.remove(fichero)

# ---------------------------------------------------------------------------
# Gestión de jobs
# ---------------------------------------------------------------------------

def _job_runner(bd: str, ruta: int, usuario: str, password: str) -> None:
    with _states_lock:
        _job_states[(bd, ruta)] = {
            "status": "ejecutando...",
            "message": "",
            "started_at": datetime.utcnow().isoformat(),
            "ended_at": "",
        }
    try:
        subir_pedidos_ruta(bd, ruta, usuario, password)
        with _states_lock:
            _job_states[(bd, ruta)]["status"] = "subido con éxito"
            _job_states[(bd, ruta)]["ended_at"] = datetime.utcnow().isoformat()
    except Exception as e:
        with _states_lock:
            _job_states[(bd, ruta)]["status"] = "error"
            _job_states[(bd, ruta)]["message"] = str(e)
            _job_states[(bd, ruta)]["ended_at"] = datetime.utcnow().isoformat()


def _enqueue_job(bd: str, ruta: int, usuario: str, password: str) -> None:
    with _states_lock:
        st = _job_states.get((bd, ruta))
        if st and st.get("status") == "ejecutando...":
            return
        _job_states.setdefault(
            (bd, ruta),
            {"status": "pendiente", "message": "", "started_at": "", "ended_at": ""},
        )
    _executor.submit(_job_runner, bd, ruta, usuario, password)

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------

@subir_pedidos_bp.route("/subir-pedidos", methods=["GET"])
@login_required
def subir_pedidos_index():
    _ensure_table()
    bd = _get_bd()
    vehiculos = _get_vehiculos(bd)
    return render_template("subir_pedidos.html", vehiculos=vehiculos, bd=bd)


@subir_pedidos_bp.route("/vehiculos/placa", methods=["POST"])
@login_required
def guardar_placa():
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
    try:
        bd = _get_bd()
        nuevo = _add_ruta(bd)
        return jsonify(success=True, data=nuevo)
    except Exception as e:
        return jsonify(success=False, error=str(e)), 400


@subir_pedidos_bp.route("/vehiculos/play", methods=["POST"])
@login_required
def ejecutar_ruta():
    try:
        data = request.get_json() or {}
        bd = _get_bd()
        ruta = int(data.get("ruta"))
        usuario = data.get("usuario", "")
        password = data.get("contrasena", "")
        _enqueue_job(bd, ruta, usuario, password)
        return jsonify(success=True)
    except Exception as e:
        return jsonify(success=False, error=str(e)), 400


@subir_pedidos_bp.route("/vehiculos/estado", methods=["GET"])
@login_required
def estado_ruta():
    bd = request.args.get("bd") or _get_bd()
    ruta = int(request.args.get("ruta", 0))
    with _states_lock:
        st = _job_states.get((bd, ruta), {"status": "pendiente"})
    return jsonify(success=True, data={"status": st.get("status"), "message": st.get("message", "")})
