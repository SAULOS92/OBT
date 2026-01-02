"""Vistas para gestionar rutas y placas de vehículos.

Este módulo contiene la lógica de Flask que administra rutas/placas y una
automatización con Playwright. El objetivo es que cualquier persona (incluso
quienes recién empiezan) pueda leerlo y entenderlo: por eso se agregaron
explicaciones paso a paso y nombres de variables descriptivos.
"""

import tempfile
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import Blueprint, jsonify, render_template, request, session
from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import sync_playwright
from openpyxl import Workbook

from db import conectar
from views.auth import login_required

subir_pedidos_bp = Blueprint("subir_pedidos", __name__, template_folder="../templates")


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


def _set_react_value(page, selector: str, value: str) -> None:
    """Escribe en un campo HTML disparando eventos de React.

    Playwright cuenta con :meth:`page.fill`, pero algunos formularios hechos con
    React no detectan el cambio de valor a menos que se disparen manualmente los
    eventos ``input`` y ``change``. Este helper hace exactamente eso.
    """

    page.evaluate(
        """([sel, val]) => {
            const element = document.querySelector(sel);
            if (!element) {
                throw new Error(`No se encontró el selector ${sel}`);
            }

            element.focus();

            // Se obtiene el setter nativo para escribir en el input.
            const setter =
              Object.getOwnPropertyDescriptor(element, 'value')?.set ||
              Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;

            const previous = element.value;
            setter.call(element, val);

            // React almacena el valor anterior en _valueTracker; lo actualizamos.
            if (element._valueTracker) {
                element._valueTracker.setValue(previous);
            }

            // Disparamos eventos para que cualquier listener se ejecute.
            element.dispatchEvent(new Event('input',  { bubbles: true }));
            element.dispatchEvent(new Event('change', { bubbles: true }));
            element.blur();
        }""",
        [selector, value],
    )


def _esperar_resultado(
    page,
    selector_exito: str,
    selector_error: Optional[str],
    total_timeout_ms: int = 20_000,
) -> Optional[bool]:
    """Espera a que aparezca un selector de éxito o error.

    Devuelve ``True`` si se detecta el selector de éxito, ``False`` si aparece el de
    error. Si no aparece ninguno durante el tiempo límite devuelve ``None`` para que
    el llamador decida cómo continuar.
    """

    deadline = time.time() + (total_timeout_ms / 1000)

    while time.time() < deadline:
        if page.query_selector(selector_exito):
            return True
        if selector_error and page.query_selector(selector_error):
            return False
        page.wait_for_timeout(300)

    return None


def ejecutar_flujo_playwright(
    pasos: List[Dict[str, Any]],
    *,
    nombre_flujo: str,
    base_url: str,
    selector_exito: str,
    selector_error: Optional[str] = None,
    notificar_estado=None,
    headless: bool = True,
    espera_resultado_ms: int = 20_000,
) -> bool:
    """Ejecuta un flujo de Playwright definido por pasos.

    Args:
        pasos: Lista de instrucciones. Cada elemento es un diccionario con las
            claves ``nombre`` (texto descriptivo), ``tipo`` ("click", "campo" o
            "campo de seleccion"), ``selector`` (CSS del elemento objetivo) y
            ``valor`` (texto a escribir u opción a seleccionar cuando aplique).
        nombre_flujo: Nombre amistoso utilizado para mensajes de error.
        base_url: URL inicial a la que se debe navegar.
        selector_exito: Selector CSS que indica que el flujo terminó bien.
        selector_error: Selector opcional que indica un estado de error visible.
        notificar_estado: Función opcional para informar el avance del flujo.
        headless: Indica si el navegador se abre en modo headless.
        espera_resultado_ms: Tiempo máximo para esperar el selector de éxito/error.

    Returns:
        ``True`` si se detecta el selector de éxito. ``False`` si se detecta el de
        error o si el tiempo de espera se agota.

    Raises:
        ValueError: Si alguno de los pasos no tiene el formato esperado.
        Exception: Propaga excepciones de Playwright una vez tomada la captura.
    """

    def _emit(mensaje: str) -> None:
        if notificar_estado:
            notificar_estado(mensaje)

    archivos_temporales: List[Path] = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()

        try:
            page.goto(base_url, wait_until="domcontentloaded", timeout=60_000)

            _emit(f"{nombre_flujo} - Navegador listo")

            for paso in pasos:
                nombre_paso = paso.get("nombre", "Paso sin nombre")
                tipo = str(paso.get("tipo", "")).strip().lower()
                selector = paso.get("selector")
                valor = paso.get("valor", "")
                
                if not selector:
                    raise ValueError(f"El paso '{nombre_paso}' no tiene selector")

                # Esperamos a que el elemento esté presente antes de interactuar.
                page.wait_for_selector(selector, timeout=30_000)

                _emit(f"{nombre_flujo} - {nombre_paso}")

                if tipo == "campo":
                    _set_react_value(page, selector, str(valor))
                elif tipo == "campo de seleccion":
                    # Para selects estándar Playwright ofrece select_option.
                    if isinstance(valor, str) and valor.lower() == "segunda opcion":
                        page.select_option(selector, index=1)
                    else:
                        page.select_option(selector, str(valor))
                elif tipo == "ingreso archivo":
                    valor_original = valor
                    ruta_archivo = valor() if callable(valor) else valor
                    ruta_archivo = Path(ruta_archivo)
                    if not ruta_archivo.exists():
                        raise ValueError(f"El archivo a subir no existe: {ruta_archivo}")
                    if callable(valor_original):
                        archivos_temporales.append(ruta_archivo)
                    try:
                        page.set_input_files(selector, ruta_archivo)
                    except Exception:
                        contenedor = page.query_selector(selector)
                        input_file = None
                        if contenedor:
                            input_file = contenedor.query_selector("input[type='file']")
                        if not input_file:
                            input_file = page.query_selector(f"{selector} input[type='file']")
                        if not input_file:
                            raise
                        input_file.set_input_files(ruta_archivo)
                elif tipo == "click":
                    page.click(selector)
                else:
                    raise ValueError(
                        "Tipo de paso inválido: debe ser 'click', 'campo', "
                        "'ingreso archivo' o 'campo de seleccion'"
                    )

            resultado = _esperar_resultado(
                page,
                selector_exito=selector_exito,
                selector_error=selector_error,
                total_timeout_ms=espera_resultado_ms,
            )

            if resultado is None:
                return False

            return bool(resultado)

        except PWTimeout:
            return False

        except Exception:
            raise

        finally:
            context.close()
            browser.close()
            for archivo in archivos_temporales:
                try:
                    archivo.unlink()
                except Exception:
                    pass


def _crear_excel_carga_pedido() -> Path:
    """Genera el archivo Excel con la estructura solicitada."""

    wb = Workbook()
    ws = wb.active

    # Se dejan dos filas vacías para replicar el formato de la imagen.
    ws["A4"] = "codigo_producto"
    ws["B4"] = "producto"
    ws["C4"] = "UN"
    ws["D4"] = "pedir"

    ws["A5"] = "1015235"
    ws["B5"] = "2 SALCH. SUN"
    ws["C5"] = "1"
    ws["D5"] = "1"

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    wb.save(tmp.name)
    tmp.close()
    return Path(tmp.name)


def login_portal_grupo_nutresa(
    username: str = "",
    password: str = "",
    base_url: str = "https://portal.gruponutresa.com",
    notificar_estado=None,
    headless: bool = True,
) -> bool:
    """Automatiza el inicio de sesión en el portal de Grupo Nutresa.

    Esta función construye un arreglo de pasos entendible para alguien que recién
    comienza con Playwright y lo envía a :func:`ejecutar_flujo_playwright`. Cada
    paso indica qué hacer (escribir, seleccionar o clickear) y sobre qué
    elemento.
    """

    pasos_login = [
        {
            "nombre": "Ingresar usuario",
            "tipo": "campo",
            "selector": "#usuario",
            "valor": username,
        },
        {
            "nombre": "Ingresar contraseña",
            "tipo": "campo",
            "selector": "#password",
            "valor": password,
        },
        {
            "nombre": "Enviar formulario",
            "tipo": "click",
            "selector": "[data-testid='SignInButton'], button[type='submit']",
        },
    ]

    selector_exito = (
        "#root > div > section > header > section > section "
        "> article.customer-header__my-business > section > button > img"
    )
    selector_error = (
        "body > div.MuiDialog-root.MuiModal-root.css-126xj0f > "
        "div.MuiDialog-container.MuiDialog-scrollPaper.css-ekeie0 > div "
        "> div.MuiDialogContent-root.css-1ty026z"
    )

    return ejecutar_flujo_playwright(
        pasos_login,
        nombre_flujo="Login Portal Grupo Nutresa",
        base_url=base_url,
        selector_exito=selector_exito,
        selector_error=selector_error,
        notificar_estado=notificar_estado,
        headless=headless,
    )


def cargar_pedido_grupo_nutresa(
    *,
    base_url: str = "https://portal.gruponutresa.com/p/nuevo/pedido-masivo/excel",
    notificar_estado=None,
    headless: bool = True,
) -> bool:
    """Carga el archivo Excel con el pedido masivo luego del login exitoso."""

    pasos_carga = [
        {
            "nombre": "Seleccionar tipo plantilla",
            "tipo": "campo de seleccion",
            "selector": (
                "#root > div > section > article > section > section > form > "
                "div > fieldset > article > div > div"
            ),
            "valor": "segunda opcion",
        },
        {
            "nombre": "Ingresar contraseña",
            "tipo": "ingreso archivo",
            "selector": (
                "#root > div > section > article > section > section > form > "
                "section > label > section.file-input__upload"
            ),
            "valor": _crear_excel_carga_pedido,
        },
        {
            "nombre": "enviar archivo",
            "tipo": "click",
            "selector": (
                "#root > div > section > article > section > section > form > "
                "footer > button.MuiButtonBase-root.MuiButton-root.MuiButton-text"
                ".MuiButton-textPrimary.MuiButton-sizeMedium.MuiButton-textSizeMedium"
                ".MuiButton-root.MuiButton-text.MuiButton-textPrimary.MuiButton-sizeMedium"
                ".MuiButton-textSizeMedium.mb2.admin__create-footer-save.w5-ns.css-kp69xf"
            ),
        },
    ]

    selector_exito = (
        "#root > div > section > article > section > article > footer > button:nth-child(3)"
    )
    selector_error = (
        "body > div.MuiDialog-root.MuiModal-root.css-126xj0f > "
        "div.MuiDialog-container.MuiDialog-scrollPaper.css-ekeie0 > div > "
        "div.MuiDialogContent-root.css-1ty026z"
    )

    return ejecutar_flujo_playwright(
        pasos_carga,
        nombre_flujo="Cargar pedido",
        base_url=base_url,
        selector_exito=selector_exito,
        selector_error=selector_error,
        notificar_estado=notificar_estado,
        headless=headless,
    )


@subir_pedidos_bp.route("/subir-pedidos/login-portal", methods=["POST"])
@login_required
def probar_login_portal():
    """Ejecuta la automatización de login con las credenciales ingresadas."""

    data = request.get_json() or {}
    username = (data.get("usuario") or "").strip()
    password = data.get("contrasena") or ""

    if not username or not password:
        return jsonify(success=False, message="Usuario y contraseña son obligatorios."), 400

    try:
        avances: List[str] = []

        ok = login_portal_grupo_nutresa(
            username=username, password=password, notificar_estado=avances.append
        )

        if ok:
            avances.append("Login exitoso - iniciando carga de pedido")
            carga_ok = cargar_pedido_grupo_nutresa(notificar_estado=avances.append)
            ok = ok and carga_ok
            message = "Pedido cargado correctamente" if carga_ok else "Login ok pero fallo la carga"
        else:
            message = "Fallo el login: revisa credenciales o selectores"
        return jsonify(success=ok, message=message, avances=avances)
    except Exception as e:
        tb = traceback.format_exc()
        print("ERROR PLAYWRIGHT LOGIN\n", tb)
        return jsonify(
            success=False,
            message="Error al ejecutar la automatización",
            error=str(e),
            traceback=tb,
        ), 500

