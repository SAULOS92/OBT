"""Automatiza la carga de pedidos al portal corporativo.

El módulo está diseñado para ejecutarse de forma asincrónica mediante
trabajos (jobs) que se colocan en una cola.  Cada job abre un navegador
con Selenium, realiza el login en el portal de Nutresa y ejecuta los
pasos necesarios para subir un archivo de pedidos.  El objetivo de esta
documentación es que cualquier principiante pueda entender, a grandes
rasgos, qué hace cada parte del archivo.
"""

import os
import json
import queue
import threading
import pathlib
from datetime import datetime
import socket
from urllib.parse import urlparse
from typing import Dict, Tuple, Optional
import logging

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_LOGIN_URL = "https://portal.gruponutresa.com/"
DEFAULT_SUCCESS_SELECTOR = (
    "#root > div > section > header > section > section > "
    "article.customer-header__my-business > section > button > img"
)
LOGIN_USER_SELECTOR = "#usuario"
LOGIN_PASS_SELECTOR = "#password"
LOGIN_SUBMIT_SELECTOR = "[data-testid='SignInButton'], button[type='submit']"
CHROME_BINARY = "/usr/bin/chromium"


class CancelledError(Exception):
    """Señala que un job fue cancelado."""


class JobControl:
    """Guarda el estado de control de cada job.

    Cada proceso de carga necesita saber si fue cancelado y cuál es el
    driver de Selenium asociado.  Esta clase agrupa esa información para
    manejarla de forma sencilla dentro de la cola de trabajos.
    """

    def __init__(self):
        # Evento que nos permite comunicar una cancelación entre hilos
        self.cancel_event = threading.Event()
        # Referencia al navegador de Selenium para poder cerrarlo si es necesario
        self.driver: Optional[webdriver.Chrome] = None


# worker de un solo hilo y cola FIFO
_job_queue: "queue.Queue[Tuple[str, int, str, str, JobControl]]" = queue.Queue()
_states_lock = threading.Lock()
_job_states: Dict[Tuple[str, int], Dict[str, Optional[str]]] = {}
_job_controls: Dict[Tuple[str, int], JobControl] = {}
_login_task_lock = threading.Lock()


def _worker_loop() -> None:
    """Hilo principal que procesa la cola de trabajos.

    Toma un job de la cola, actualiza su estado y ejecuta la rutina
    completa de subida de pedidos.  Si el job se cancela o falla se
    refleja en el diccionario ``_job_states`` para poder consultarlo
    desde la interfaz web.
    """

    while True:
        # Esperamos hasta obtener un nuevo trabajo
        bd, ruta, usuario, password, control = _job_queue.get()
        # Protegemos el acceso a los estados con un lock
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
                    "failed_step": None,
                },
            )
            st.update({"status": "ejecutando", "paso_actual": None, "failed_step": None, "started_at": datetime.utcnow().isoformat(), "ended_at": ""})
        try:
            # Ejecuta la lógica de carga de pedidos
            subir_pedidos_ruta(bd, ruta, usuario, password, control)
            with _states_lock:
                st = _job_states[(bd, ruta)]
                st["ended_at"] = datetime.utcnow().isoformat()
        except CancelledError:
            # El job fue cancelado por el usuario
            with _states_lock:
                st = _job_states[(bd, ruta)]
                st["ended_at"] = datetime.utcnow().isoformat()
        except Exception:  # pragma: no cover - selenium/portal interaction
            # Ante cualquier error se marca el job como fallido
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
            # El job termina: limpiamos su control y avisamos a la cola
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
    """Obtiene de la sesión la base de datos seleccionada."""

    bd = session.get("empresa")
    if not bd:
        raise ValueError("Falta empresa en sesión")
    return bd


def _obtener_placa(bd: str, ruta: int) -> str:
    """Consulta la placa asociada a una ruta en la tabla ``vehiculos``."""

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT placa FROM vehiculos WHERE bd=%s AND ruta=%s",
                (bd, ruta),
            )
            row = cur.fetchone()
    return row[0] if row else ""


def _get_vehiculos(bd: str):
    """Devuelve todas las rutas y placas registradas para una empresa."""

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ruta, placa FROM vehiculos WHERE bd=%s ORDER BY ruta", (bd,))
            rows = cur.fetchall()
            # Si no hay registros, creamos uno por defecto
            if not rows:
                cur.execute(
                    "INSERT INTO vehiculos (bd, ruta, placa) VALUES (%s, %s, %s)",
                    (bd, 1, ""),
                )
                conn.commit()
                rows = [(1, "")]
    vehiculos = []
    for r in rows:
        # Leemos el estado actual del job (si existe)
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
# Selenium config y helpers de estado
# ---------------------------------------------------------------------------


# Selectores CSS y URLs utilizadas durante la automatización
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
    "canal.value": "20",
    "placa.input": "#formValue",
    "carrito.confirmar": "#root > div > section > article > section > section > section.cart__resume-options > button:nth-child(3)",
    "respuesta.aceptar": "#root > div > section > article > section > section > section > section.order__confirmation-products > article.order__confirmation-products-button > button",
}


def _set_state(bd: str, ruta: int, **kwargs) -> None:
    """Actualiza la información de estado de un job."""

    with _states_lock:
        st = _job_states.setdefault(
            (bd, ruta),
            {
                "status": "pendiente",
                "paso_actual": None,
                "started_at": "",
                "ended_at": "",
                "failed_step": None,
            },
        )
        st.update(kwargs)


def _set_step(bd: str, ruta: int, step: Optional[str]) -> None:
    """Registra el paso actual que está ejecutando el job."""

    _set_state(bd, ruta, paso_actual=step)
    if step:
        logger.info("Ruta %s: %s", ruta, _step_desc(step))


def _current_step(bd: str, ruta: int) -> Optional[str]:
    """Devuelve el nombre del paso en curso para una ruta."""

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
    """Lanza ``CancelledError`` si el usuario solicitó cancelar."""

    if control.cancel_event.is_set():
        raise CancelledError()


def crear_excel(bd: str, ruta: int) -> pathlib.Path:
    """Genera el archivo Excel de pedidos para una ruta."""

    return selenium_crear_excel_pedidos(bd, ruta)


def cleanup(path: pathlib.Path) -> None:
    """Elimina archivos temporales si existen."""

    if path.exists():
        os.remove(path)


# ---------------------------------------------------------------------------
# Login on-demand (headless)
# ---------------------------------------------------------------------------


def make_driver() -> webdriver.Chrome:  # pragma: no cover - selenium/portal interaction
    """Crea un driver de Chrome configurado para Render."""

    options = Options()
    options.binary_location = CHROME_BINARY
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,720")
    service = Service()
    return webdriver.Chrome(options=options, service=service)


def set_react_value(driver: webdriver.Chrome, selector: str, value: str) -> bool:
    """Establece un valor en un input de React disparando eventos."""

    script = """
const [sel, val] = arguments;
const el = document.querySelector(sel);
if (!el) { return false; }
const setter = Object.getOwnPropertyDescriptor(el, 'value')?.set ||
  Object.getOwnPropertyDescriptor(Object.getPrototypeOf(el), 'value')?.set ||
  Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
const prev = el.value;
el.focus();
setter.call(el, val);
if (el._valueTracker) { el._valueTracker.setValue(prev); }
el.dispatchEvent(new Event('input', { bubbles: true }));
el.dispatchEvent(new Event('change', { bubbles: true }));
el.blur();
return true;
"""
    return bool(driver.execute_script(script, selector, value))


def login(driver: webdriver.Chrome, user: str, password: str) -> None:
    """Ejecuta el flujo de login usando Selenium y React value setter."""

    login_url = os.getenv("LOGIN_URL", DEFAULT_LOGIN_URL)
    driver.get(login_url)
    wait = WebDriverWait(driver, 30)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, LOGIN_USER_SELECTOR)))
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, LOGIN_PASS_SELECTOR)))
    if not set_react_value(driver, LOGIN_USER_SELECTOR, user):
        raise ValueError("No se encontró el input de usuario")
    if not set_react_value(driver, LOGIN_PASS_SELECTOR, password):
        raise ValueError("No se encontró el input de contraseña")
    submit = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, LOGIN_SUBMIT_SELECTOR))
    )
    driver.execute_script(
        """
const btn = arguments[0];
if (!btn) return;
const form = btn.closest('form');
if (form && typeof form.requestSubmit === 'function') {
  form.requestSubmit(btn);
} else {
  btn.click();
}
""",
        submit,
    )


def assert_login_success(driver: webdriver.Chrome) -> bool:
    """Espera el selector configurado de éxito y devuelve True si aparece."""

    selector = os.getenv("SUCCESS_SELECTOR", DEFAULT_SUCCESS_SELECTOR)
    wait = WebDriverWait(driver, 30)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
    return True


def run_login_task() -> Dict[str, Optional[str]]:
    """Orquesta el login headless y devuelve el resultado serializable."""

    result: Dict[str, Optional[str]] = {
        "passed": False,
        "url": None,
        "title": None,
        "error": None,
        "screenshot": None,
    }
    driver: Optional[webdriver.Chrome] = None
    try:
        user = os.getenv("LOGIN_USER")
        password = os.getenv("LOGIN_PASS")
        if not user or not password:
            raise ValueError("LOGIN_USER o LOGIN_PASS no configurados")
        driver = make_driver()
        login(driver, user, password)
        assert_login_success(driver)
        result.update({
            "passed": True,
            "url": driver.current_url,
            "title": driver.title,
        })
    except Exception as exc:  # pragma: no cover - selenium/portal interaction
        result.update({"error": str(exc)})
        if driver:
            result["url"] = driver.current_url
            result["title"] = driver.title
            screenshot_path = f"/tmp/error_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.png"
            try:
                driver.save_screenshot(screenshot_path)
                result["screenshot"] = screenshot_path
            except Exception:
                result["screenshot"] = None
    finally:
        if driver:
            driver.quit()
    return result


# ---------------------------------------------------------------------------
# Funciones Selenium por paso
# ---------------------------------------------------------------------------


def build_driver() -> webdriver.Chrome:  # pragma: no cover - requiere driver
    """Configura y crea una instancia de Chrome para Selenium."""

    options = Options()
    # Ejecutamos en modo headless para no abrir una ventana visible
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    # Habilitamos los logs del navegador para depurar
    options.set_capability("goog:loggingPrefs", {"browser": "ALL"})
    log_name = f"chromedriver_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"
    service = Service(log_path=log_name)
    return webdriver.Chrome(options=options, service=service)


def selenium_login(driver, config, usuario: str, password: str) -> None:
    """Realiza el login en el portal usando un script probado en el navegador.

    El formulario del portal está hecho con React y no siempre responde a
    ``send_keys``.  Para garantizar que los valores se carguen y se
    disparen los eventos correspondientes inyectamos un pequeño script en
    la página.  Las credenciales provienen de la interfaz del usuario y se
    pasan como argumentos a este script, sin quedar hardcodeadas.
    """

    driver.get(config["login.url"])
    # Esperamos a que los campos estén presentes antes de ejecutar el script
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["login.user"]))
    )
    script = """
const [userSel, passSel, userVal, passVal] = arguments;
const setReactValue = (sel, val) => {
  const el = document.querySelector(sel);
  if (!el) return;
  el.focus();
  const setter = Object.getOwnPropertyDescriptor(el, 'value')?.set ||
                 Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
  const prev = el.value;
  setter.call(el, val);
  if (el._valueTracker) el._valueTracker.setValue(prev);
  el.dispatchEvent(new Event('input', { bubbles: true }));
  el.dispatchEvent(new Event('change', { bubbles: true }));
  el.blur();
};

setReactValue(userSel, userVal);
setReactValue(passSel, passVal);
const btn = document.querySelector('[data-testid="SignInButton"], button[type="submit"]');
const form = btn?.closest('form');
if (form?.requestSubmit) form.requestSubmit(btn);
else btn?.click();
"""
    driver.execute_script(script, config["login.user"], config["login.pass"], usuario, password)
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["login.ok"]))
    )


def selenium_crear_excel_pedidos(bd: str, ruta: int) -> pathlib.Path:
    """Genera un Excel con los pedidos de la ruta indicada."""

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT fn_obtener_pedidos_con_pedir_json(%s);", (bd,))
            pedidos = json.loads(cur.fetchone()[0])

    registros = [p for p in pedidos if p["ruta"] == ruta]
    if not registros:
        raise ValueError(f"Sin pedidos para ruta {ruta}")

    df = pd.DataFrame(registros)[["codigo_pro", "producto", "pedir"]]
    df.insert(2, "UN", "UN")  # Columna requerida por el formato
    tmp = pathlib.Path(f"{ruta}.xlsx")
    df.to_excel(tmp, sheet_name="Pedidos", startrow=4, index=False, engine="openpyxl")
    return tmp


def selenium_ingreso_modulo_de_carga(driver, config) -> None:
    """Abre la página del módulo de carga de archivos."""

    driver.get(config["modulo.url"])
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, config["modulo.ok"]))
    )


def selenium_cargando_el_archivo(driver, config, xls_path) -> None:
    """Carga el archivo Excel en el formulario del portal siguiendo el script proporcionado."""

    # Paso 3: seleccionar el tipo de carga
    script = """
(() => {
  const trigger = document.querySelector(arguments[0]);
  if (!trigger) return;
  trigger.click();
  const t0 = Date.now();
  const id = setInterval(() => {
    const opts = document.querySelectorAll("ul[role='listbox'] li[role='option']");
    if (opts.length >= 2) {
      opts[1].click();
      clearInterval(id);
    } else if (Date.now() - t0 > 3000) {
      clearInterval(id);
    }
  }, 100);
})();
"""
    driver.execute_script(script, config["modulo.ok"])

    # Paso 4: habilitar el input de archivo y cargar el formato
    script = """
(() => {
  const input = document.querySelector(arguments[0]);
  if (!input) return;
  input.removeAttribute('disabled');
  input.style.display = 'block';
  input.style.visibility = 'visible';
  input.style.opacity = 1;
})();
"""
    driver.execute_script(script, config["carga.fileInput"])
    driver.find_element(By.CSS_SELECTOR, config["carga.fileInput"]).send_keys(str(xls_path))

    # Paso 5: guardar el formato
    script = """
(() => {
  const btn = document.querySelector(arguments[0]);
  if (!btn) return;
  btn.removeAttribute('disabled');
  btn.classList.remove('Mui-disabled');
  btn.click();
})();
"""
    driver.execute_script(script, config["carga.guardar"])

    # Paso 6: agregar productos al carrito
    script = """
(() => {
  const btn = document.querySelector(arguments[0]);
  if (!btn) return;
  btn.removeAttribute('disabled');
  btn.classList.remove('Mui-disabled');
  btn.click();
})();
"""
    driver.execute_script(script, config["carga.continuar"])


def selenium_agregando_al_carrito(driver, config, placa: str) -> None:
    """Abre el carrito y completa la orden de compra y la placa."""

    # Paso 7: ir al resumen del carrito
    driver.get("https://portal.gruponutresa.com/carrito/resumen")

    # Paso 8 y 9: rellenar orden de compra y placa
    script = """
(() => {
  const el = document.querySelector(arguments[0]);
  if (!el) return;
  el.focus();
  const setter = Object.getOwnPropertyDescriptor(el, 'value')?.set ||
                 Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
  const prev = el.value;
  setter.call(el, arguments[1]);
  if (el._valueTracker) el._valueTracker.setValue(prev);
  el.dispatchEvent(new Event('input', { bubbles: true }));
  el.dispatchEvent(new Event('change', { bubbles: true }));
  el.blur();
})();
"""
    driver.execute_script(script, config["canal.input"], config["canal.value"])
    driver.execute_script(script, config["placa.input"], placa)


def selenium_aceptando_el_pedido(driver, config) -> None:
    """Confirma el carrito antes de finalizar el pedido."""

    script = """
(() => {
  const btn = document.querySelector(arguments[0]);
  if (!btn) return;
  btn.removeAttribute('disabled');
  btn.classList.remove('Mui-disabled');
  btn.click();
  const key = Object.keys(btn).find(k => k.startsWith('__reactProps$'));
  const props = key && btn[key];
  if (props?.onClick) {
    props.onClick({ type: 'click', preventDefault(){}, stopPropagation(){} });
  }
})();
"""
    driver.execute_script(script, config["carrito.confirmar"])


def selenium_confirmando_el_pedido(driver, config) -> Dict[str, str]:
    """Confirma definitivamente el pedido."""

    script = """
(() => {
  const btn = document.querySelector(arguments[0]);
  if (!btn) return;
  btn.removeAttribute('disabled');
  btn.classList.remove('Mui-disabled');
  btn.click();
  const key = Object.keys(btn).find(k => k.startsWith('__reactProps$'));
  const props = key && btn[key];
  if (props?.onClick) {
    props.onClick({ type: 'click', preventDefault(){}, stopPropagation(){} });
  }
})();
"""
    driver.execute_script(script, config["respuesta.aceptar"])
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
    """Ejecuta todos los pasos para subir los pedidos de una ruta."""

    _set_state(bd, ruta, status="ejecutando", paso_actual=None, failed_step=None)

    _set_step(bd, ruta, "preparando_excel")
    xls = crear_excel(bd, ruta)

    _set_step(bd, ruta, "inicializando_driver")
    driver = build_driver()
    control.driver = driver

    try:
        try:
            _set_step(bd, ruta, "login")
            check_cancel(control)
            selenium_login(driver, CONFIG, usuario, password)
        except Exception:
            _set_state(bd, ruta, status="error", failed_step="login")
            raise

        try:
            _set_step(bd, ruta, "ingreso_al_modulo_de_carga")
            check_cancel(control)
            selenium_ingreso_modulo_de_carga(driver, CONFIG)
        except Exception:
            _set_state(bd, ruta, status="error", failed_step="ingreso_al_modulo_de_carga")
            raise

        try:
            _set_step(bd, ruta, "cargando_el_archivo")
            check_cancel(control)
            selenium_cargando_el_archivo(driver, CONFIG, xls)
        except Exception:
            _set_state(bd, ruta, status="error", failed_step="cargando_el_archivo")
            raise

        placa = _obtener_placa(bd, ruta)
        try:
            _set_step(bd, ruta, "agregando_al_carrito")
            check_cancel(control)
            selenium_agregando_al_carrito(driver, CONFIG, placa)
        except Exception:
            _set_state(bd, ruta, status="error", failed_step="agregando_al_carrito")
            raise

        try:
            _set_step(bd, ruta, "aceptando_el_pedido")
            check_cancel(control)
            selenium_aceptando_el_pedido(driver, CONFIG)
        except Exception:
            _set_state(bd, ruta, status="error", failed_step="aceptando_el_pedido")
            raise

        try:
            _set_step(bd, ruta, "confirmando_el_pedido")
            check_cancel(control)
            selenium_confirmando_el_pedido(driver, CONFIG)
            _set_state(bd, ruta, status="exito")
        except Exception:
            _set_state(bd, ruta, status="error", failed_step="confirmando_el_pedido")
            raise

    except CancelledError:
        _set_state(bd, ruta, status="cancelado")
        raise
    except Exception:
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
    """Añade un nuevo job a la cola para procesar una ruta."""

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
    """Muestra la pantalla principal para gestionar la subida de pedidos."""

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
    """Inicia el proceso de subida para una ruta específica."""

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
    """Cancela un job en ejecución o en cola para una ruta dada."""

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
    """Devuelve el estado actual de un job para una ruta."""

    bd = request.args.get("bd") or _get_bd()
    ruta = int(request.args.get("ruta", 0))
    with _states_lock:
        st = _job_states.get(
            (bd, ruta),
            {
                "status": "pendiente",
                "paso_actual": None,
                "failed_step": None,
            },
        )
    return jsonify(
        success=True,
        data={
            "status": st.get("status", "pendiente"),
            "paso": _step_desc(st.get("paso_actual")),
            "failed_step": st.get("failed_step"),
        },
    )


@subir_pedidos_bp.route("/subir-pedidos/login-check", methods=["POST"])
def login_check():
    """Ejecuta el login on-demand protegido por token."""

    run_token = os.getenv("RUN_TOKEN")
    if not run_token:
        return jsonify(success=False, error="RUN_TOKEN no configurado"), 500
    if request.headers.get("X-Run-Token") != run_token:
        return jsonify(success=False, error="Token inválido"), 401
    if not os.getenv("LOGIN_USER") or not os.getenv("LOGIN_PASS"):
        return (
            jsonify(success=False, error="LOGIN_USER o LOGIN_PASS no configurados"),
            500,
        )
    if not _login_task_lock.acquire(blocking=False):
        return jsonify(success=False, error="Ya existe una ejecución en curso"), 429

    try:
        result = run_login_task()
        status_code = 200 if result.get("passed") else 500
        payload = {
            "success": bool(result.get("passed")),
            "passed": bool(result.get("passed")),
            "url": result.get("url"),
            "title": result.get("title"),
            "error": result.get("error"),
        }
        if result.get("screenshot"):
            payload["screenshot"] = result["screenshot"]
        return jsonify(payload), status_code
    finally:
        _login_task_lock.release()


@subir_pedidos_bp.route("/vehiculos/diagnostico", methods=["GET"])
@login_required
def diagnostico_ruta():
    """Realiza pruebas básicas de conectividad hacia el portal."""

    try:
        data = probe_reachability(CONFIG)
        return jsonify(success=True, data=data)
    except Exception as e:
        return jsonify(success=False, error=str(e)), 500


