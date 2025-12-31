"""Vistas para gestionar rutas y placas de vehículos."""

import traceback

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


def login_portal_grupo_nutresa(
    username: str = "",
    password: str = "",
    base_url: str = "https://portal.gruponutresa.com",
    screenshot_path: str = "login_error.png",
    headless: bool = True,
) -> bool:
    """Automatiza el inicio de sesión en el portal de Grupo Nutresa con Playwright.

    El flujo replica el snippet recibido: completa los campos de usuario/contraseña con
    eventos compatibles con React, envía el formulario y valida la presencia de un
    selector inequívoco de éxito. Devuelve ``True`` si el login se confirma, ``False``
    en caso contrario.
    """

    SEL_USER = "#usuario"
    SEL_PASS = "#password"
    SEL_SUBMIT = "[data-testid='SignInButton'], button[type='submit']"
    SEL_SUCCESS_IMG = (
        "#root > div > section > header > section > section "
        "> article.customer-header__my-business > section > button > img"
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()

        try:
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

            page.wait_for_selector(SEL_SUCCESS_IMG, timeout=60_000)
            return True

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
    """Ejecuta la automatización de login con las credenciales ingresadas."""

    data = request.get_json() or {}
    username = (data.get("usuario") or "").strip()
    password = data.get("contrasena") or ""

    if not username or not password:
        return jsonify(success=False, message="Usuario y contraseña son obligatorios."), 400

    try:
        ok = login_portal_grupo_nutresa(username=username, password=password)
        message = "Login exitoso" if ok else "Fallo el login: revisa credenciales o selectores"
        return jsonify(success=ok, message=message)
    except Exception as e:
        tb = traceback.format_exc()
        print("ERROR PLAYWRIGHT LOGIN\n", tb)
        return jsonify(
            success=False,
            message="Error al ejecutar la automatización",
            error=str(e),
            traceback=tb,
        ), 500

