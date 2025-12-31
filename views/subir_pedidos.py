"""Vistas para gestionar rutas y placas de vehículos."""

import tempfile
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional

from flask import Blueprint, jsonify, render_template, request, session
from openpyxl import Workbook
from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import sync_playwright

from db import conectar
from views.auth import login_required

subir_pedidos_bp = Blueprint("subir_pedidos", __name__, template_folder="../templates")

PEDIDO_MASIVO_URL = "https://portal.gruponutresa.com/p/nuevo/pedido-masivo/excel"
SEL_USER = "#usuario"
SEL_PASS = "#password"
SEL_SUBMIT = "[data-testid='SignInButton'], button[type='submit']"
SEL_SUCCESS_IMG = (
    "#root > div > section > header > section > section "
    "> article.customer-header__my-business > section > button > img"
)
SEL_ERROR_DIALOG = (
    "body > div.MuiDialog-root.MuiModal-root.css-126xj0f > "
    "div.MuiDialog-container.MuiDialog-scrollPaper.css-ekeie0 > div "
    "> div.MuiDialogContent-root.css-1ty026z"
)


def _ensure_table() -> None:
    """Crea la tabla `vehiculos` si aún no existe."""

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
    """Obtiene la empresa activa desde la sesión."""

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
    return [{"ruta": r, "placa": p} for r, p in rows]


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


def _add_ruta(bd: str):
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


def _delete_ruta(bd: str, ruta: int) -> bool:
    """Elimina la ruta indicada. Devuelve True si alguna fila fue borrada."""

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM vehiculos WHERE bd=%s AND ruta=%s", (bd, ruta))
            deleted = cur.rowcount > 0
            conn.commit()
    return deleted


@subir_pedidos_bp.route("/subir-pedidos", methods=["GET"])
@login_required
def subir_pedidos_index():
    """Pantalla principal para consultar y editar placas."""

    _ensure_table()
    bd = _get_bd()
    vehiculos = _get_vehiculos(bd)
    return render_template("subir_pedidos.html", vehiculos=vehiculos, bd=bd)


@subir_pedidos_bp.route("/vehiculos/placa", methods=["POST"])
@login_required
def guardar_placa():
    """Guarda la placa asociada a una ruta enviada desde la tabla."""

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


@subir_pedidos_bp.route("/vehiculos/delete", methods=["POST"])
@login_required
def eliminar_ruta():
    """Elimina una ruta específica."""

    try:
        data = request.get_json() or {}
        bd = _get_bd()
        ruta = int(data.get("ruta"))
        deleted = _delete_ruta(bd, ruta)
        return jsonify(success=deleted, data={"ruta": ruta})
    except Exception as e:
        return jsonify(success=False, error=str(e)), 400


def _wait_for_login_result(page, total_timeout_ms: int = 20_000):
    """Devuelve True si se ve el selector de éxito, False si aparece el diálogo de error."""

    deadline = time.time() + (total_timeout_ms / 1000)

    while time.time() < deadline:
        if page.query_selector(SEL_SUCCESS_IMG):
            return True
        if page.query_selector(SEL_ERROR_DIALOG):
            return False
        page.wait_for_timeout(300)

    return None


def _perform_login(
    page,
    username: str,
    password: str,
    base_url: str,
    log: Optional[Callable[[str], None]] = None,
):
    """Ejecuta el login en el portal y devuelve ``True`` si fue exitoso."""

    def _log(msg: str):
        if log:
            log(msg)

    _log("Abriendo el portal de Grupo Nutresa")
    page.goto(base_url, wait_until="domcontentloaded", timeout=60_000)

    page.wait_for_selector(SEL_USER, timeout=30_000)
    page.wait_for_selector(SEL_PASS, timeout=30_000)

    page.evaluate(
        """([user, passw]) => {
            const setReactValue = (sel, val) => {
                const el = document.querySelector(sel);
                if (!el) return;
                el.focus();
                const setter =
                  Object.getOwnPropertyDescriptor(el, 'value')?.set ||
                  Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
                const prev = el.value;
                setter.call(el, val);
                if (el._valueTracker) el._valueTracker.setValue(prev);
                el.dispatchEvent(new Event('input',  { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
                el.blur();
            };

            setReactValue('#usuario', user);
            setReactValue('#password', passw);

            const btn =
              document.querySelector("[data-testid='SignInButton'], button[type='submit']");
            const form = btn?.closest('form');
            if (form?.requestSubmit) form.requestSubmit(btn);
            else btn?.click();
        }""",
        [username, password],
    )

    _log("Formulario enviado, esperando confirmación de login")
    return _wait_for_login_result(page, total_timeout_ms=20_000)


def _build_excel_desde_imagen(tmp_dir: Optional[Path] = None) -> Path:
    """Construye el Excel a subir con las tres primeras filas en blanco.

    El contenido replica el formato de referencia: deja filas vacías y carga ejemplos
    de clientes y productos listos para ser agregados al carrito.
    """

    destino = Path(tmp_dir or tempfile.gettempdir()) / "pedido_masivo_generado.xlsx"
    wb = Workbook()
    ws = wb.active

    # Tres filas iniciales completamente en blanco
    ws.append([])
    ws.append([])
    ws.append([])

    # Encabezados y datos tal cual se observan en la imagen de referencia
    headers = ["codigo_pro", "producto", "UN", "pedir"]
    filas = [
        [1015235, "2 SALCH. SP. RANCHERA X 120G", "UN", 1],
        [1075657, "CHORIZO RICA X 175 G", "UN", 5],
    ]

    ws.append(headers)
    for fila in filas:
        ws.append(fila)

    wb.save(destino)
    return destino


@dataclass
class PortalFlowResult:
    success: bool
    message: str
    logs: List[str]
    screenshot: Optional[str] = None


def ejecutar_flujo_pedido_masivo(
    username: str,
    password: str,
    base_url: str = "https://portal.gruponutresa.com",
    headless: bool = True,
) -> PortalFlowResult:
    """Ejecuta el login y luego los pasos de carga masiva descritos por el usuario."""

    logs: List[str] = []
    screenshot_path = Path(tempfile.gettempdir()) / "pedido_masivo_error.png"

    def log(msg: str) -> None:
        logs.append(msg)
        print(f"[SUBIR_PEDIDOS] {msg}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()

        try:
            result = _perform_login(page, username, password, base_url, log)
            if result is True:
                log("Login confirmado")
            elif result is False:
                log("El portal mostró un diálogo de error al autenticar")
                return PortalFlowResult(False, "Falló el login en el portal", logs)
            else:
                page.screenshot(path=screenshot_path, full_page=True)
                return PortalFlowResult(
                    False,
                    "No fue posible confirmar el login dentro del tiempo esperado",
                    logs,
                    str(screenshot_path),
                )

            log("Ingresando a carga de pedidos masivos")
            page.goto(PEDIDO_MASIVO_URL, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1200)

            seleccion = page.evaluate(
                """() => {
                    const trigger = document.querySelector("#root > div > section > article > section > section > form > div > fieldset > article > div > div");
                    if (!trigger) return "❌ combo no encontrado";
                    trigger.click();

                    const t0 = Date.now();
                    const id = setInterval(() => {
                        const opts = document.querySelectorAll("ul[role='listbox'] li[role='option']");
                        if (opts.length >= 2) {
                            opts[1].click();
                            clearInterval(id);
                            console.log("✅ segunda opción:", opts[1].innerText.trim());
                        } else if (Date.now() - t0 > 3000) {
                            clearInterval(id);
                            console.log("⚠️ no apareció la segunda opción");
                        }
                    }, 100);
                    return "Tipo de carga: intento de seleccionar segunda opción";
                }""",
            )
            log(seleccion or "Intentando seleccionar tipo de carga")

            excel_path = _build_excel_desde_imagen()
            file_input = page.query_selector("#root form input[type='file']")
            if file_input:
                file_input.set_input_files(str(excel_path))
                log(f"Excel generado y asignado al input: {excel_path.name}")
            else:
                log("❌ no se encontró el input[type=file] para subir el Excel")
                return PortalFlowResult(False, "No se encontró el input file", logs)

            guardar_btn = page.query_selector(
                "#root > div > section > article > section > section > form > footer > button.MuiButtonBase-root.MuiButton-root.MuiButton-text.MuiButton-textPrimary.MuiButton-sizeMedium.MuiButton-textSizeMedium.MuiButton-root.MuiButton-text.MuiButton-textPrimary.MuiButton-sizeMedium.MuiButton-textSizeMedium.mb2.admin__create-footer-save.w5-ns.css-kp69xf"
            )
            if guardar_btn:
                guardar_btn.evaluate("(btn) => { btn.removeAttribute('disabled'); btn.classList.remove('Mui-disabled'); }")
                guardar_btn.click()
                log("Click forzado en el botón Guardar para cargar el formato")
                page.wait_for_timeout(1500)
            else:
                log("❌ Botón Guardar no encontrado")

            carrito_btn = page.query_selector(
                "#root > div > section > article > section > article > footer > button:nth-child(3)"
            )
            if carrito_btn:
                carrito_btn.evaluate("(btn) => { btn.removeAttribute('disabled'); btn.classList.remove('Mui-disabled'); }")
                carrito_btn.click()
                log("Productos agregados al carrito")
            else:
                log("❌ Botón 'Agregar al carrito' no encontrado")

            return PortalFlowResult(True, "Flujo de carga masiva completado", logs)

        except PWTimeout:
            page.screenshot(path=screenshot_path, full_page=True)
            log("Timeout en alguna de las acciones de Playwright")
            return PortalFlowResult(False, "Timeout durante la automatización", logs, str(screenshot_path))

        except Exception:
            page.screenshot(path=screenshot_path, full_page=True)
            log("Error inesperado durante la automatización")
            raise

        finally:
            context.close()
            browser.close()


def login_portal_grupo_nutresa(
    username: str = "",
    password: str = "",
    base_url: str = "https://portal.gruponutresa.com",
    screenshot_path: str = "login_error.png",
    headless: bool = True,
) -> bool:
    """Automatiza el inicio de sesión en el portal de Grupo Nutresa con Playwright."""

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()

        try:
            result = _perform_login(page, username, password, base_url)
            if result is True:
                return True
            if result is False:
                return False

            # No se detectó ni éxito ni error: captura para diagnóstico y responde False.
            page.screenshot(path=screenshot_path, full_page=True)
            return False

        except PWTimeout:
            page.screenshot(path=screenshot_path, full_page=True)
            return False

        except Exception:
            page.screenshot(path=screenshot_path, full_page=True)
            raise

        finally:
            context.close()
            browser.close()


@subir_pedidos_bp.route("/subir-pedidos/login-portal", methods=["POST"])
@login_required
def probar_login_portal():
    """Ejecuta el login y la continuación de carga masiva de pedidos."""

    data = request.get_json() or {}
    username = (data.get("usuario") or "").strip()
    password = data.get("contrasena") or ""

    if not username or not password:
        return jsonify(success=False, message="Usuario y contraseña son obligatorios."), 400

    try:
        # Primero validar el login como en la versión original.
        login_ok = login_portal_grupo_nutresa(
            username=username,
            password=password,
            base_url="https://portal.gruponutresa.com",
            headless=True,
        )

        if not login_ok:
            return jsonify(
                success=False,
                message="Fallo el login: revisa credenciales o selectores",
                logs=["No se pudo autenticar en el portal"],
            )

        # Si el login funcionó, continuar con la automatización completa.
        resultado = ejecutar_flujo_pedido_masivo(
            username=username,
            password=password,
            logs=["Login confirmado, continuando con la carga masiva"],
        )
        return jsonify(
            success=resultado.success,
            message=resultado.message,
            logs=resultado.logs,
            screenshot=resultado.screenshot,
        )
    except Exception as e:
        tb = traceback.format_exc()
        print("ERROR PLAYWRIGHT LOGIN\n", tb)
        return jsonify(
            success=False,
            message="Error al ejecutar la automatización",
            error=str(e),
            traceback=tb,
        ), 500

