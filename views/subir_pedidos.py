import os
import json
import queue
import threading
import pathlib
from datetime import datetime
from typing import Dict, Tuple, Optional

import pandas as pd
from flask import Blueprint, render_template, request, session, jsonify
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from db import conectar
from views.auth import login_required

# ---------------------------------------------------------------------------
# Configuración global
# ---------------------------------------------------------------------------

subir_pedidos_bp = Blueprint("subir_pedidos", __name__, template_folder="../templates")

# worker de un solo hilo y cola FIFO
_job_queue: "queue.Queue[Tuple[str, int, str, str]]" = queue.Queue()
_states_lock = threading.Lock()
_job_states: Dict[Tuple[str, int], Dict[str, Optional[str]]] = {}


def _worker_loop() -> None:
    while True:
        bd, ruta, usuario, password = _job_queue.get()
        with _states_lock:
            st = _job_states.setdefault(
                (bd, ruta),
                {
                    "status": "pendiente",
                    "current_step": None,
                    "started_at": "",
                    "ended_at": "",
                    "message": "",
                    "failed_step": None,
                },
            )
            st["status"] = "ejecutando"
            st["started_at"] = datetime.utcnow().isoformat()
        try:
            subir_pedidos_ruta(bd, ruta, usuario, password)
            with _states_lock:
                st = _job_states[(bd, ruta)]
                st["status"] = "exito"
                st["ended_at"] = datetime.utcnow().isoformat()
        except Exception as e:  # pragma: no cover - selenium/portal interaction
            with _states_lock:
                st = _job_states[(bd, ruta)]
                st["status"] = "error"
                st["message"] = str(e)
                st["failed_step"] = st.get("current_step")
                st["ended_at"] = datetime.utcnow().isoformat()
        finally:
            _job_queue.task_done()


_worker_thread = threading.Thread(target=_worker_loop, daemon=True)
_worker_thread.start()


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


def _obtener_placa(bd: str, ruta: int) -> str:
    conn = conectar()
    cur = conn.cursor()
    cur.execute(
        "SELECT placa FROM vehiculos WHERE bd=%s AND ruta=%s",
        (bd, ruta),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else ""


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
            {
                "ruta": r[0],
                "placa": r[1],
                "estado": _status_text(st.get("status", "pendiente")),
                "paso": st.get("current_step"),
            }
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
    return {"ruta": next_ruta, "placa": "", "estado": "pendiente", "paso": None}


# ---------------------------------------------------------------------------
# Selenium config y helpers de estado
# ---------------------------------------------------------------------------


CONFIG = {
    "modulo.url": "https://portal.gruponutresa.com/p/nuevo/pedido-masivo/excel",
    "login.user": "#usuario",
    "login.pass": "#password",
    "login.submit": "#root > section > section > div.auth__layout-container-center.overflow-x-hidden.overflow-y-auto > div > div > form > button",
    "login.ok": "#root > div > section > header > section > section > article.customer-header__my-business > section > button",
    "modulo.ok": "#root > div > section > article > section > section > form > div > fieldset > article > div > div",
    "carga.combo1": "#root > div > section > article > section > section > form > div > fieldset > article:nth-child(1) > div > div",
    "carga.combo2": "#root > div > section > article > section > section > form > div > fieldset > article:nth-child(2) > div > div",
    "carga.fileLabel": "#root > div > section > article > section > section > form > section > label > section.file-input__label-text",
    "carga.fileInput": "form input[type='file']",
    "carga.guardar": "#root > div > section > article > section > section > form > footer > button.MuiButtonBase-root.MuiButton-root.MuiButton-text.MuiButton-textPrimary.MuiButton-sizeMedium.MuiButton-textSizeMedium.MuiButton-root.MuiButton-text.MuiButton-textPrimary.MuiButton-sizeMedium.MuiButton-textSizeMedium.mb2.admin__create-footer-save.w5-ns.css-kp69xf",
    "carga.continuar": "#root > div > section > article > section > article > footer > button:nth-child(3) > span.MuiButton-startIcon.MuiButton-iconSizeMedium.css-6xugel > svg",
    "canal.input": "#purchaseOrderNN13CANALT",
    "canal.value": "14",
    "placa.input": "#formValue",
    "carrito.confirmar": "#root > div > section > article > section > section > section.cart__resume-options > button:nth-child(3)",
    "respuesta.aceptar": "#root > div > section > article > section > section > section > section.order__confirmation-products > article.order__confirmation-products-button > button",
}


def _status_text(status: str) -> str:
    return {
        "pendiente": "pendiente",
        "en cola": "en cola",
        "ejecutando": "ejecutando…",
        "exito": "subido con éxito",
        "error": "error",
    }.get(status, status)


def _set_state(bd: str, ruta: int, **kwargs) -> None:
    with _states_lock:
        st = _job_states.setdefault(
            (bd, ruta),
            {
                "status": "pendiente",
                "current_step": None,
                "started_at": "",
                "ended_at": "",
                "message": "",
                "failed_step": None,
            },
        )
        st.update(kwargs)


def _set_step(bd: str, ruta: int, step: Optional[str]) -> None:
    _set_state(bd, ruta, current_step=step)


def _current_step(bd: str, ruta: int) -> Optional[str]:
    with _states_lock:
        return _job_states.get((bd, ruta), {}).get("current_step")


# ---------------------------------------------------------------------------
# Funciones Selenium por paso
# ---------------------------------------------------------------------------


def build_driver() -> webdriver.Chrome:  # pragma: no cover - requiere driver
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)


def selenium_login(driver, config, usuario: str, password: str) -> None:
    driver.get(config["modulo.url"])
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["login.user"]))
    ).send_keys(usuario)
    driver.find_element(By.CSS_SELECTOR, config["login.pass"]).send_keys(password)
    driver.find_element(By.CSS_SELECTOR, config["login.submit"]).click()
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["login.ok"]))
    )


def selenium_crear_excel_pedidos(bd: str, ruta: int) -> pathlib.Path:
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
    tmp = pathlib.Path(f"{ruta}.xlsx")
    df.to_excel(tmp, sheet_name="Pedidos", startrow=4, index=False, engine="openpyxl")
    return tmp


def selenium_ir_modulo_carga(driver, config) -> None:
    driver.get(config["modulo.url"])
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["modulo.ok"]))
    )


def selenium_cargar_excel(driver, config, xls_path) -> None:
    try:
        driver.find_element(By.CSS_SELECTOR, config["carga.combo1"]).click()
        driver.find_element(By.CSS_SELECTOR, config["carga.combo2"]).click()
    except Exception:
        pass
    file_input = driver.find_element(By.CSS_SELECTOR, config["carga.fileInput"])
    file_input.send_keys(str(xls_path))
    driver.find_element(By.CSS_SELECTOR, config["carga.guardar"]).click()
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["carga.continuar"]))
    ).click()


def selenium_set_canal(driver, config) -> None:
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["canal.input"]))
    ).send_keys(config["canal.value"])


def selenium_ir_carrito(driver, config) -> None:
    driver.get("https://portal.gruponutresa.com/carrito/resumen")


def selenium_anotar_placas(driver, config, placa: str) -> None:
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["placa.input"]))
    ).send_keys(placa)


def selenium_confirmar_pedido(driver, config) -> None:
    WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, config["carrito.confirmar"]))
    ).click()


def selenium_confirmar_respuesta(driver, config) -> Dict[str, str]:
    try:
        WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, config["respuesta.aceptar"]))
        ).click()
    except Exception:
        pass
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Orquestación
# ---------------------------------------------------------------------------


def subir_pedidos_ruta(bd: str, ruta: int, usuario: str, password: str) -> None:
    _set_step(bd, ruta, "crear_excel")
    xls = selenium_crear_excel_pedidos(bd, ruta)
    driver = build_driver()
    try:
        _set_step(bd, ruta, "login")
        selenium_login(driver, CONFIG, usuario, password)

        _set_step(bd, ruta, "ir_modulo")
        selenium_ir_modulo_carga(driver, CONFIG)

        _set_step(bd, ruta, "cargar_excel")
        selenium_cargar_excel(driver, CONFIG, xls)

        _set_step(bd, ruta, "set_canal")
        selenium_set_canal(driver, CONFIG)

        _set_step(bd, ruta, "ir_carrito")
        selenium_ir_carrito(driver, CONFIG)

        placa = _obtener_placa(bd, ruta)
        _set_step(bd, ruta, "anotar_placas")
        selenium_anotar_placas(driver, CONFIG, placa)

        _set_step(bd, ruta, "confirmar_pedido")
        selenium_confirmar_pedido(driver, CONFIG)

        _set_step(bd, ruta, "confirmar_respuesta")
        res = selenium_confirmar_respuesta(driver, CONFIG)
        _set_state(bd, ruta, message=json.dumps(res))
    finally:  # pragma: no cover - cleanup
        try:
            driver.quit()
        finally:
            if xls.exists():
                os.remove(xls)


# ---------------------------------------------------------------------------
# Gestión de jobs
# ---------------------------------------------------------------------------


def _enqueue_job(bd: str, ruta: int, usuario: str, password: str) -> str:
    with _states_lock:
        st = _job_states.get((bd, ruta))
        if st and st.get("status") in {"ejecutando", "en cola"}:
            return _status_text(st["status"])
        status = "ejecutando" if _job_queue.empty() else "en cola"
        _job_states[(bd, ruta)] = {
            "status": status,
            "current_step": None,
            "started_at": datetime.utcnow().isoformat() if status == "ejecutando" else "",
            "ended_at": "",
            "message": "",
            "failed_step": None,
        }
    _job_queue.put((bd, ruta, usuario, password))
    return _status_text(status)


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
        usuario = data.get("usuario")
        password = data.get("contrasena")
        if not usuario or not password:
            return jsonify(success=False, error="Credenciales requeridas"), 400
        status = _enqueue_job(bd, ruta, usuario, password)
        return jsonify(success=True, data={"status": status})
    except Exception as e:
        return jsonify(success=False, error=str(e)), 400


@subir_pedidos_bp.route("/vehiculos/estado", methods=["GET"])
@login_required
def estado_ruta():
    bd = request.args.get("bd") or _get_bd()
    ruta = int(request.args.get("ruta", 0))
    with _states_lock:
        st = _job_states.get(
            (bd, ruta),
            {
                "status": "pendiente",
                "current_step": None,
                "message": "",
                "failed_step": None,
            },
        )
    return jsonify(
        success=True,
        data={
            "status": _status_text(st.get("status", "pendiente")),
            "paso": st.get("current_step"),
            "failed_step": st.get("failed_step"),
            "message": st.get("message", ""),
        },
    )


