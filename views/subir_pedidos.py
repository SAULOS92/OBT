import os
import json
import queue
import threading
import pathlib
from datetime import datetime
import socket
from urllib.parse import urlparse
from typing import Dict, Tuple, Optional

import pandas as pd
import requests
from flask import Blueprint, render_template, request, session, jsonify
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from db import conectar
from views.auth import login_required

# ---------------------------------------------------------------------------
# Configuración global
# ---------------------------------------------------------------------------

subir_pedidos_bp = Blueprint("subir_pedidos", __name__, template_folder="../templates")


class CancelledError(Exception):
    """Señala que un job fue cancelado."""


class JobControl:
    def __init__(self):
        self.cancel_event = threading.Event()
        self.driver: Optional[webdriver.Chrome] = None


# worker de un solo hilo y cola FIFO
_job_queue: "queue.Queue[Tuple[str, int, str, str, JobControl]]" = queue.Queue()
_states_lock = threading.Lock()
_job_states: Dict[Tuple[str, int], Dict[str, Optional[str]]] = {}
_job_controls: Dict[Tuple[str, int], JobControl] = {}


def _log(bd: str, ruta: int, msg: str) -> None:
    ts = datetime.utcnow().isoformat()
    with _states_lock:
        st = _job_states.setdefault(
            (bd, ruta),
            {
                "status": "pendiente",
                "paso_actual": None,
                "started_at": "",
                "ended_at": "",
                "message": "",
                "failed_step": None,
            },
        )
        st["message"] = (st.get("message", "") + f"[{ts}] {msg}\n")[:10000]


def _screenshot(driver: webdriver.Chrome, ruta: int, tag: str) -> None:
    fname = f"snap_{ruta}_{tag}.png"
    try:
        driver.save_screenshot(fname)
    except Exception:
        pass


def _worker_loop() -> None:
    while True:
        bd, ruta, usuario, password, control = _job_queue.get()
        with _states_lock:
            st = _job_states.get((bd, ruta))
            if st and st.get("status") == "cancelado":
                _job_queue.task_done()
                continue
            st = _job_states.setdefault(
                (bd, ruta),
                {
                    "status": "pendiente",
                    "paso_actual": None,
                    "started_at": "",
                    "ended_at": "",
                    "message": "",
                    "failed_step": None,
                },
            )
            st.update({"status": "ejecutando", "paso_actual": None, "failed_step": None, "started_at": datetime.utcnow().isoformat(), "ended_at": "", "message": ""})
        try:
            subir_pedidos_ruta(bd, ruta, usuario, password, control)
            with _states_lock:
                st = _job_states[(bd, ruta)]
                st["ended_at"] = datetime.utcnow().isoformat()
        except CancelledError:
            with _states_lock:
                st = _job_states[(bd, ruta)]
                st["ended_at"] = datetime.utcnow().isoformat()
        except Exception as e:  # pragma: no cover - selenium/portal interaction
            _log(bd, ruta, f"Error en worker: {e}")
            with _states_lock:
                st = _job_states[(bd, ruta)]
                st.update(
                    {
                        "status": "error",
                        "failed_step": st.get("paso_actual"),
                        "ended_at": datetime.utcnow().isoformat(),
                    }
                )
        finally:
            with _states_lock:
                _job_controls.pop((bd, ruta), None)
            _job_queue.task_done()


_worker_thread = threading.Thread(target=_worker_loop, daemon=True)
_worker_thread.start()


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
    bd = session.get("empresa")
    if not bd:
        raise ValueError("Falta empresa en sesión")
    return bd


def _obtener_placa(bd: str, ruta: int) -> str:
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT placa FROM vehiculos WHERE bd=%s AND ruta=%s",
                (bd, ruta),
            )
            row = cur.fetchone()
    return row[0] if row else ""


def _get_vehiculos(bd: str):
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
                "paso": _step_desc(st.get("paso_actual")),
            }
        )
    return vehiculos


def _upsert_vehiculo(bd: str, ruta: int, placa: str) -> None:
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
# Selenium config y helpers de estado
# ---------------------------------------------------------------------------


CONFIG = {
    "login.url": "https://portal.gruponutresa.com/",
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


def _set_state(bd: str, ruta: int, **kwargs) -> None:
    with _states_lock:
        st = _job_states.setdefault(
            (bd, ruta),
            {
                "status": "pendiente",
                "paso_actual": None,
                "started_at": "",
                "ended_at": "",
                "message": "",
                "failed_step": None,
            },
        )
        st.update(kwargs)


def _set_step(bd: str, ruta: int, step: Optional[str]) -> None:
    _set_state(bd, ruta, paso_actual=step)


def _current_step(bd: str, ruta: int) -> Optional[str]:
    with _states_lock:
        return _job_states.get((bd, ruta), {}).get("paso_actual")


STEP_DESCRIPTIONS = {
    "preparando_excel": "Preparando Excel",
    "inicializando_driver": "Inicializando driver",
    "login": "Iniciando sesión",
    "ingreso_al_modulo_de_carga": "Ingresando al módulo de carga",
    "cargando_el_archivo": "Cargando el archivo",
    "agregando_al_carrito": "Agregando al carrito",
    "aceptando_el_pedido": "Aceptando el pedido",
    "confirmando_el_pedido": "Confirmando el pedido",
}


def _step_desc(step: Optional[str]) -> Optional[str]:
    """Traduce el nombre interno del paso a uno legible."""
    if not step:
        return None
    return STEP_DESCRIPTIONS.get(step, step)




def check_cancel(control: JobControl) -> None:
    if control.cancel_event.is_set():
        raise CancelledError()


def crear_excel(bd: str, ruta: int) -> pathlib.Path:
    return selenium_crear_excel_pedidos(bd, ruta)


def cleanup(path: pathlib.Path) -> None:
    if path.exists():
        os.remove(path)


# ---------------------------------------------------------------------------
# Funciones Selenium por paso
# ---------------------------------------------------------------------------


def build_driver() -> webdriver.Chrome:  # pragma: no cover - requiere driver
    options = Options()
    # options.add_argument("--headless")  # uncomment to see the UI
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.set_capability("goog:loggingPrefs", {"browser": "ALL"})
    log_name = f"chromedriver_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"
    service = Service(log_path=log_name)
    return webdriver.Chrome(options=options, service=service)


def selenium_login(driver, config, usuario: str, password: str) -> None:
    driver.get(config["login.url"])
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["login.user"]))
    ).send_keys(usuario)
    driver.find_element(By.CSS_SELECTOR, config["login.pass"]).send_keys(password)
    driver.find_element(By.CSS_SELECTOR, config["login.submit"]).click()
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["login.ok"]))
    )


def selenium_crear_excel_pedidos(bd: str, ruta: int) -> pathlib.Path:
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT fn_obtener_pedidos_con_pedir_json(%s);", (bd,))
            pedidos = json.loads(cur.fetchone()[0])

    registros = [p for p in pedidos if p["ruta"] == ruta]
    if not registros:
        raise ValueError(f"Sin pedidos para ruta {ruta}")

    df = pd.DataFrame(registros)[["codigo_pro", "producto", "pedir"]]
    df.insert(2, "UN", "UN")
    tmp = pathlib.Path(f"{ruta}.xlsx")
    df.to_excel(tmp, sheet_name="Pedidos", startrow=4, index=False, engine="openpyxl")
    return tmp


def selenium_ingreso_modulo_de_carga(driver, config) -> None:
    driver.get(config["modulo.url"])
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["modulo.ok"]))
    )


def selenium_cargando_el_archivo(driver, config, xls_path) -> None:
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


def selenium_agregando_al_carrito(driver, config, placa: str) -> None:
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["canal.input"]))
    ).send_keys(config["canal.value"])
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, config["placa.input"]))
        ).send_keys(placa)
    except Exception:
        driver.get("https://portal.gruponutresa.com/carrito/resumen")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, config["placa.input"]))
        ).send_keys(placa)


def selenium_aceptando_el_pedido(driver, config) -> None:
    driver.get("https://portal.gruponutresa.com/carrito/resumen")


def selenium_confirmando_el_pedido(driver, config) -> Dict[str, str]:
    WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, config["carrito.confirmar"]))
    ).click()
    try:
        WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, config["respuesta.aceptar"]))
        ).click()
    except Exception:
        pass
    return {"status": "ok"}


def probe_reachability(config) -> Dict[str, Dict[str, str]]:
    """Diagnóstico básico de red"""
    result: Dict[str, Dict[str, str]] = {}
    host = urlparse(config["login.url"]).hostname or ""
    try:
        ip = socket.gethostbyname(host)
        result["dns"] = {"host": host, "ip": ip}
    except Exception as e:
        result["dns"] = {"host": host, "error": str(e)}

    for key in ("login.url", "modulo.url"):
        url = config[key]
        try:
            r = requests.get(url, timeout=10)
            result[key] = {"status": str(r.status_code)}
        except Exception as e:
            result[key] = {"error": str(e)}
    return result


# ---------------------------------------------------------------------------
# Orquestación
# ---------------------------------------------------------------------------


def subir_pedidos_ruta(bd: str, ruta: int, usuario: str, password: str, control: JobControl) -> None:
    _set_state(bd, ruta, status="ejecutando", paso_actual=None, failed_step=None)

    _set_step(bd, ruta, "preparando_excel")
    _log(bd, ruta, "Preparando Excel")
    xls = crear_excel(bd, ruta)

    _set_step(bd, ruta, "inicializando_driver")
    _log(bd, ruta, "Inicializando driver")
    driver = build_driver()
    control.driver = driver
    _screenshot(driver, ruta, "inicializando_driver")

    try:
        try:
            _set_step(bd, ruta, "login")
            _log(bd, ruta, "Iniciando login")
            check_cancel(control)
            selenium_login(driver, CONFIG, usuario, password)
            _screenshot(driver, ruta, "login")
            _log(bd, ruta, "Login completado")
        except Exception as e:
            _screenshot(driver, ruta, "login_error")
            _log(bd, ruta, f"Error en login: {e}")
            _set_state(bd, ruta, status="error", failed_step="login")
            raise

        try:
            _set_step(bd, ruta, "ingreso_al_modulo_de_carga")
            _log(bd, ruta, "Ingresando al módulo de carga")
            check_cancel(control)
            selenium_ingreso_modulo_de_carga(driver, CONFIG)
            _screenshot(driver, ruta, "ingreso_al_modulo_de_carga")
        except Exception as e:
            _screenshot(driver, ruta, "ingreso_al_modulo_de_carga_error")
            _log(bd, ruta, f"Error ingresando al módulo de carga: {e}")
            _set_state(bd, ruta, status="error", failed_step="ingreso_al_modulo_de_carga")
            raise

        try:
            _set_step(bd, ruta, "cargando_el_archivo")
            _log(bd, ruta, "Cargando el archivo")
            check_cancel(control)
            selenium_cargando_el_archivo(driver, CONFIG, xls)
            _screenshot(driver, ruta, "cargando_el_archivo")
        except Exception as e:
            _screenshot(driver, ruta, "cargando_el_archivo_error")
            _log(bd, ruta, f"Error cargando el archivo: {e}")
            _set_state(bd, ruta, status="error", failed_step="cargando_el_archivo")
            raise

        placa = _obtener_placa(bd, ruta)
        try:
            _set_step(bd, ruta, "agregando_al_carrito")
            _log(bd, ruta, "Agregando al carrito")
            check_cancel(control)
            selenium_agregando_al_carrito(driver, CONFIG, placa)
            _screenshot(driver, ruta, "agregando_al_carrito")
        except Exception as e:
            _screenshot(driver, ruta, "agregando_al_carrito_error")
            _log(bd, ruta, f"Error agregando al carrito: {e}")
            _set_state(bd, ruta, status="error", failed_step="agregando_al_carrito")
            raise

        try:
            _set_step(bd, ruta, "aceptando_el_pedido")
            _log(bd, ruta, "Aceptando el pedido")
            check_cancel(control)
            selenium_aceptando_el_pedido(driver, CONFIG)
            _screenshot(driver, ruta, "aceptando_el_pedido")
        except Exception as e:
            _screenshot(driver, ruta, "aceptando_el_pedido_error")
            _log(bd, ruta, f"Error aceptando el pedido: {e}")
            _set_state(bd, ruta, status="error", failed_step="aceptando_el_pedido")
            raise

        try:
            _set_step(bd, ruta, "confirmando_el_pedido")
            _log(bd, ruta, "Confirmando el pedido")
            check_cancel(control)
            res = selenium_confirmando_el_pedido(driver, CONFIG)
            _screenshot(driver, ruta, "confirmando_el_pedido")
            _log(bd, ruta, f"Pedido confirmado: {json.dumps(res)}")
            _set_state(bd, ruta, status="exito")
        except Exception as e:
            _screenshot(driver, ruta, "confirmando_el_pedido_error")
            _log(bd, ruta, f"Error confirmando el pedido: {e}")
            _set_state(bd, ruta, status="error", failed_step="confirmando_el_pedido")
            raise

    except CancelledError:
        _log(bd, ruta, "Job cancelado")
        _set_state(bd, ruta, status="cancelado")
        raise
    except Exception as e:
        _log(bd, ruta, f"Error general: {e}")
        _set_state(bd, ruta, status="error", failed_step=_current_step(bd, ruta))
        raise
    finally:  # pragma: no cover - cleanup
        try:
            driver.quit()
        except Exception:
            pass
        finally:
            cleanup(xls)


# ---------------------------------------------------------------------------
# Gestión de jobs
# ---------------------------------------------------------------------------


def _enqueue_job(bd: str, ruta: int, usuario: str, password: str) -> str:
    with _states_lock:
        st = _job_states.get((bd, ruta))
        if st and st.get("status") in {"ejecutando", "en cola"}:
            return st["status"]
        status = "ejecutando" if _job_queue.empty() else "en cola"
        _job_states[(bd, ruta)] = {
            "status": status,
            "paso_actual": None,
            "started_at": datetime.utcnow().isoformat() if status == "ejecutando" else "",
            "ended_at": "",
            "message": "",
            "failed_step": None,
        }
        control = JobControl()
        _job_controls[(bd, ruta)] = control
    _job_queue.put((bd, ruta, usuario, password, control))
    return status


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


@subir_pedidos_bp.route("/vehiculos/stop", methods=["POST"])
@login_required
def detener_ruta():
    try:
        data = request.get_json() or {}
        bd = _get_bd()
        ruta = int(data.get("ruta"))
        with _states_lock:
            st = _job_states.get((bd, ruta))
            if not st:
                return jsonify(success=True, data={"status": "pendiente"})
            if st.get("status") == "en cola":
                st["status"] = "cancelado"
            elif st.get("status") == "ejecutando":
                st["status"] = "cancelado"
                control = _job_controls.get((bd, ruta))
                if control:
                    control.cancel_event.set()
                    if control.driver:
                        try:
                            control.driver.quit()
                        except Exception:
                            pass
        with _states_lock:
            status = _job_states.get((bd, ruta), {}).get("status", "pendiente")
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
                "paso_actual": None,
                "message": "",
                "failed_step": None,
            },
        )
    return jsonify(
        success=True,
        data={
            "status": st.get("status", "pendiente"),
            "paso": _step_desc(st.get("paso_actual")),
            "failed_step": st.get("failed_step"),
            "message": st.get("message", ""),
        },
    )


@subir_pedidos_bp.route("/vehiculos/diagnostico", methods=["GET"])
@login_required
def diagnostico_ruta():
    try:
        data = probe_reachability(CONFIG)
        return jsonify(success=True, data=data)
    except Exception as e:
        return jsonify(success=False, error=str(e)), 500


@subir_pedidos_bp.route("/vehiculos/log", methods=["GET"])
@login_required
def obtener_log():
    bd = request.args.get("bd") or _get_bd()
    ruta = int(request.args.get("ruta", 0))
    with _states_lock:
        msg = _job_states.get((bd, ruta), {}).get("message", "")
    return jsonify(success=True, data={"message": msg})


