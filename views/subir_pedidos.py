"""Vistas para gestionar rutas y placas de vehículos.

Este módulo contiene la lógica de Flask que administra rutas/placas y una
automatización con Playwright. El objetivo es que cualquier persona (incluso
quienes recién empiezan) pueda leerlo y entenderlo: por eso se agregaron
explicaciones paso a paso y nombres de variables descriptivos.
"""

import time
import traceback
from typing import Any, Dict, List, Optional

from flask import Blueprint, jsonify, render_template, request, session
from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import sync_playwright

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


def _seleccionar_segunda_opcion(page, selector: str) -> None:
    """Selecciona la segunda opción disponible de un elemento <select>."""

    option_value = page.evaluate(
        """(sel) => {
            const element = document.querySelector(sel);
            if (!element) throw new Error(`No se encontró el selector ${sel}`);
            const segunda = element.options?.[1];
            if (!segunda) throw new Error('No existe una segunda opción en el select');
            return segunda.value;
        }""",
        selector,
    )
    page.select_option(selector, option_value)


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
            claves ``nombre`` (texto descriptivo), ``tipo`` ("click", "campo",
            "campo de seleccion" o "ingreso archivo"), ``selector`` (CSS del
            elemento objetivo) y ``valor`` (texto a escribir u opción a
            seleccionar cuando aplique, o un callable que devuelva la ruta de
            archivo a subir).
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

                valor_resuelto = valor() if callable(valor) else valor

                if tipo == "campo":
                    _set_react_value(page, selector, str(valor_resuelto))
                elif tipo == "campo de seleccion":
                    if str(valor_resuelto).lower() == "segunda opcion":
                        _seleccionar_segunda_opcion(page, selector)
                    else:
                        # Para selects estándar Playwright ofrece select_option.
                        page.select_option(selector, str(valor_resuelto))
                elif tipo == "ingreso archivo":
                    if not valor_resuelto:
                        raise ValueError(
                            "El paso de ingreso de archivo requiere una ruta o un constructor"
                        )
                    page.set_input_files(selector, valor_resuelto)
                elif tipo == "click":
                    page.click(selector)
                else:
                    raise ValueError(
                        "Tipo de paso inválido: debe ser 'click', 'campo', "
                        "'campo de seleccion' o 'ingreso archivo'"
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


def construir_archivo_carga_pedido() -> str:
    """Construye un Excel con el formato esperado por el portal."""

    from tempfile import NamedTemporaryFile

    from openpyxl import Workbook

    workbook = Workbook()
    sheet = workbook.active

    # Dejar dos filas iniciales vacías para replicar la plantilla del portal.
    sheet.append([None])
    sheet.append([None])
    sheet.append(["codigo_pro", "producto", "UN", "pedir"])
    sheet.append(["1015235", "2 SALCH. SUN", None, 1])

    tmp = NamedTemporaryFile(delete=False, suffix=".xlsx")
    workbook.save(tmp.name)
    return tmp.name


def cargar_pedido_masivo(
    *,
    notificar_estado=None,
    headless: bool = True,
) -> bool:
    """Ejecuta el flujo de carga masiva de pedidos en el portal."""

    archivo_temporal = {"path": None}

    def _crear_archivo():
        archivo_temporal["path"] = construir_archivo_carga_pedido()
        return archivo_temporal["path"]

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
            "valor": _crear_archivo,
        },
        {
            "nombre": "enviar archivo",
            "tipo": "click",
            "selector": (
                "#root > div > section > article > section > section > form > "
                "footer > button.MuiButtonBase-root.MuiButton-root.MuiButton-text."
                "MuiButton-textPrimary.MuiButton-sizeMedium.MuiButton-textSizeMedium."
                "MuiButton-root.MuiButton-text.MuiButton-textPrimary.MuiButton-"
                "sizeMedium.MuiButton-textSizeMedium.mb2.admin__create-footer-save."
                "w5-ns.css-kp69xf"
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

    try:
        return ejecutar_flujo_playwright(
            pasos_carga,
            nombre_flujo="Cargar pedido",
            base_url="https://portal.gruponutresa.com/p/nuevo/pedido-masivo/excel",
            selector_exito=selector_exito,
            selector_error=selector_error,
            notificar_estado=notificar_estado,
            headless=headless,
        )
    finally:
        if archivo_temporal["path"]:
            try:
                import os

                os.unlink(archivo_temporal["path"])
            except OSError:
                pass


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

        ok_login = login_portal_grupo_nutresa(
            username=username, password=password, notificar_estado=avances.append
        )

        ok_carga = False
        if ok_login:
            ok_carga = cargar_pedido_masivo(notificar_estado=avances.append)

        if ok_login and ok_carga:
            message = "Login y carga de pedido exitosos"
        elif ok_login and not ok_carga:
            message = "Login exitoso pero falló la carga de pedido"
        else:
            message = "Fallo el login: revisa credenciales o selectores"

        return jsonify(success=ok_login and ok_carga, message=message, avances=avances)
    except Exception as e:
        tb = traceback.format_exc()
        print("ERROR PLAYWRIGHT LOGIN\n", tb)
        return jsonify(
            success=False,
            message="Error al ejecutar la automatización",
            error=str(e),
            traceback=tb,
        ), 500

