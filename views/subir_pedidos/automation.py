"""Funciones de automatización basadas en Playwright."""

import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional

from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import sync_playwright


def _set_react_value(page, selector: str, value: str) -> None:
    """Escribe en un campo HTML disparando eventos de React."""

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
    """Espera a que aparezca un selector de éxito o error."""

    deadline = time.time() + (total_timeout_ms / 1000)

    while time.time() < deadline:
        if page.query_selector(selector_exito):
            return True
        if selector_error and page.query_selector(selector_error):
            return False
        page.wait_for_timeout(300)

    return None


@contextmanager
def iniciar_navegador(*, headless: bool = True):
    """Inicializa y entrega una página de navegador lista para usar."""

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()

        try:
            yield page
        finally:
            context.close()
            browser.close()


def ejecutar_flujo_playwright(
    pasos: List[Dict[str, Any]],
    *,
    nombre_flujo: str,
    selector_exito: str,
    selector_error: Optional[str] = None,
    notificar_estado=None,
    headless: bool = True,
    espera_resultado_ms: int = 20_000,
) -> bool:
    """Ejecuta un flujo de Playwright definido por pasos."""

    try:
        with iniciar_navegador(headless=headless) as page:
            return ejecutar_flujo_en_pagina(
                page,
                pasos,
                nombre_flujo=nombre_flujo,
                selector_exito=selector_exito,
                selector_error=selector_error,
                notificar_estado=notificar_estado,
                espera_resultado_ms=espera_resultado_ms,
            )

    except PWTimeout:
        return False

    except Exception:
        raise


def ejecutar_flujo_en_pagina(
    page,
    pasos: List[Dict[str, Any]],
    *,
    nombre_flujo: str,
    selector_exito: str,
    selector_error: Optional[str] = None,
    notificar_estado: Optional[Callable[[str], None]] = None,
    espera_resultado_ms: int = 20_000,
) -> bool:
    """Ejecuta un flujo reutilizando una página ya abierta."""

    def _emit(mensaje: str) -> None:
        if notificar_estado:
            notificar_estado(mensaje)

    _emit(f"{nombre_flujo} - Navegador listo")

    for paso in pasos:
        nombre_paso = paso.get("nombre", "Paso sin nombre")
        tipo = str(paso.get("tipo", "")).strip().lower()
        selector = paso.get("selector")
        valor = paso.get("valor", "")
        script = paso.get("script")

        requiere_selector = tipo not in {"navegar", "campo de seleccion", "archivo"}
        if requiere_selector and not selector:
            raise ValueError(f"El paso '{nombre_paso}' no tiene selector")

        _emit(f"{nombre_flujo} - {nombre_paso}")

        if tipo == "navegar":
            page.goto(str(valor), wait_until="domcontentloaded", timeout=60_000)
        elif tipo == "campo":
            page.wait_for_selector(selector, timeout=30_000)
            _set_react_value(page, selector, str(valor))
        elif tipo == "campo de seleccion":
            if script:
                # Permite ejecutar lógica personalizada de selección
                # (por ejemplo, abrir combos y elegir opciones dinámicas).
                page.evaluate(script)
            else:
                if not selector:
                    raise ValueError(
                        f"El paso '{nombre_paso}' no tiene selector ni script para seleccionar"
                    )
                page.wait_for_selector(selector, timeout=30_000)
                page.select_option(selector, str(valor))
        elif tipo == "click":
            page.wait_for_selector(selector, timeout=30_000)
            page.click(selector)
        elif tipo == "archivo":
            ruta_archivo = paso.get("archivo") or valor
            if not ruta_archivo:
                raise ValueError(f"El paso '{nombre_paso}' no tiene ruta de archivo")

            input_selector = selector or "input[type='file']"
            page.wait_for_selector(input_selector, timeout=30_000)

            # Algunos portales ocultan/deshabilitan el input; el script permite
            # habilitarlo/mostrarlo antes de adjuntar el archivo si es necesario.
            if script:
                page.evaluate(script)

            page.set_input_files(input_selector, ruta_archivo)
        else:
            raise ValueError(
                "Tipo de paso inválido: debe ser 'navegar', 'click', "
                "'campo', 'campo de seleccion' o 'archivo'"
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


def login_portal_grupo_nutresa(
    username: str = "",
    password: str = "",
    base_url: str = "https://portal.gruponutresa.com",
    notificar_estado=None,
    headless: bool = True,
    page=None,
) -> bool:
    """Automatiza el inicio de sesión en el portal de Grupo Nutresa."""

    pasos_login = [
        {
            "nombre": "Ir al portal",
            "tipo": "navegar",
            "valor": base_url,
        },
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

    if page:
        return ejecutar_flujo_en_pagina(
            page,
            pasos_login,
            nombre_flujo="Login Portal Grupo Nutresa",
            selector_exito=selector_exito,
            selector_error=selector_error,
            notificar_estado=notificar_estado,
            espera_resultado_ms=20_000,
        )

    return ejecutar_flujo_playwright(
        pasos_login,
        nombre_flujo="Login Portal Grupo Nutresa",
        selector_exito=selector_exito,
        selector_error=selector_error,
        notificar_estado=notificar_estado,
        headless=headless,
    )
