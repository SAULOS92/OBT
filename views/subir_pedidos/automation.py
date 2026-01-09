"""Funciones de automatización basadas en Playwright."""

import time
from datetime import date
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

            const isTextarea = element instanceof HTMLTextAreaElement;
            const prototype = isTextarea
                ? HTMLTextAreaElement.prototype
                : HTMLInputElement.prototype;

            // Se obtiene el setter nativo para escribir en el input o textarea.
            const setter =
              Object.getOwnPropertyDescriptor(element, 'value')?.set ||
              Object.getOwnPropertyDescriptor(prototype, 'value').set;

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
    """Inicializa y entrega un contexto de navegador listo para usar."""

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-zygote",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 720}, accept_downloads=False
        )
        context.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in {"image", "media", "font"}
            else route.continue_(),
        )
        try:
            yield context
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
        with iniciar_navegador(headless=headless) as context:
            page = context.new_page()
            try:
                return ejecutar_flujo_en_pagina(
                    page,
                    pasos,
                    nombre_flujo=nombre_flujo,
                    selector_exito=selector_exito,
                    selector_error=selector_error,
                    notificar_estado=notificar_estado,
                    espera_resultado_ms=espera_resultado_ms,
                )
            finally:
                page.close()

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

    step_timeout_ms = 60_000

    def _emit(mensaje: str) -> None:
        print(mensaje, flush=True)
        if notificar_estado:
            notificar_estado(mensaje)

    def _obtener_selector(nombre_paso: str, selector: Optional[str]) -> str:
        if not selector:
            raise ValueError(f"El paso '{nombre_paso}' no tiene selector")
        return selector

    def _esperar_selector(selector: str) -> None:
        page.wait_for_selector(selector, timeout=step_timeout_ms)

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
            page.goto(str(valor), wait_until="domcontentloaded", timeout=step_timeout_ms)
        elif tipo == "campo":
            selector = _obtener_selector(nombre_paso, selector)
            _esperar_selector(selector)
            _set_react_value(page, selector, str(valor))
        elif tipo == "campo de seleccion":
            if script:
                # Permite ejecutar lógica personalizada de selección
                # (por ejemplo, abrir combos y elegir opciones dinámicas).
                page.evaluate(script)
            else:
                selector = _obtener_selector(nombre_paso, selector)
                _esperar_selector(selector)
                page.select_option(selector, str(valor))
        elif tipo == "click":
            selector = _obtener_selector(nombre_paso, selector)
            _esperar_selector(selector)
            page.click(selector, timeout=step_timeout_ms)
        elif tipo == "mousedown":
            selector = _obtener_selector(nombre_paso, selector)
            _esperar_selector(selector)
            page.dispatch_event(selector, "mousedown")
        elif tipo == "archivo":
            ruta_archivo = paso.get("archivo") or valor
            if not ruta_archivo:
                raise ValueError(f"El paso '{nombre_paso}' no tiene ruta de archivo")

            input_selector = selector or "input[type='file']"
            page.wait_for_selector(
                input_selector, state="attached", timeout=step_timeout_ms
            )

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


def cargar_pedido_masivo_excel(
    ruta_placa: Dict[str, Any],
    ruta_archivo_excel: str,
    campo_placa: str,
    *,
    notificar_estado: Optional[Callable[[str], None]] = None,
    headless: bool = True,
    page=None,
) -> bool:
    """Carga un archivo de pedido masivo en el portal Grupo Nutresa."""

    placa = str(ruta_placa.get("placa") or ruta_placa.get("ruta") or "").strip()
    if not placa:
        raise ValueError("No se pudo resolver placa/ruta")

    ruta_archivo_final = str(ruta_archivo_excel or "").strip()
    if not ruta_archivo_final:
        raise ValueError("Falta ruta_archivo_excel")

    dia_actual = str(date.today().day)

    if campo_placa == "purchase_order":
        purchase_order_value = placa
        observaciones_value = dia_actual
    elif campo_placa == "observaciones":
        observaciones_value = placa
        purchase_order_value = dia_actual
    else:
        raise ValueError("campo_placa inválido, usa 'purchase_order' u 'observaciones'")

    pasos_carga = [
        {
            "nombre": "Ir a la pagina de carga masiva",
            "tipo": "navegar",
            "valor": "https://portal.gruponutresa.com/p/nuevo/pedido-masivo/excel",
        },
        {
            "nombre": "Seleccionar tipo plantilla - abrir combo",
            "tipo": "mousedown",
            "selector": (
                "div[role='button'][aria-haspopup='listbox']:has-text('Seleccione el tipo de plantilla')"
            ),
        },
        {
            "nombre": "Seleccionar tipo plantilla - elegir plantilla estándar",
            "tipo": "click",
            "selector": (
                "ul[role='listbox'] li[role='option']:has-text('Plantilla estándar')"
            ),
        },
        {
            "nombre": "Seleccionar archivo",
            "tipo": "archivo",
            "selector": "#file",
            "valor": ruta_archivo_final,
        },
        {
            "nombre": "enviar archivo",
            "tipo": "click",
            "selector": (
                "button[type='submit'][data-testid='LoadingButton']:has-text('Guardar')"
            ),
        },
        {
            "nombre": "Agregar al carrito",
            "tipo": "click",
            "selector": "button[data-testid='NextActionButton']",
        },
        {
            "nombre": "Continuar (dialog éxito)",
            "tipo": "click",
            "selector": "button[data-testid='SuccessDialogButton']",
        },
        {
            "nombre": "Ingresar Orden de Compra",
            "tipo": "campo",
            "selector": "input#purchaseOrderNN12CANALT",
            "valor": purchase_order_value,
        },
        {
            "nombre": "Ingresar Observaciones",
            "tipo": "campo",
            "selector": "textarea[data-testid='formValue']",
            "valor": observaciones_value,
        },
        {
            "nombre": "Confirmar pedido",
            "tipo": "click",
            "selector": "button[data-testid='LoadingButton']:has-text('Confirmar pedido')",
        },
        {
            "nombre": "Finalizar pedido",
            "tipo": "click",
            "selector": "button[data-testid='OrderConfirmationPageFinishOrderButton']:has-text('Finalizar Pedido')",
        },
    ]

    selector_exito = "button[data-testid='LoadingButton']:has-text('Enviar notificación pedido')"
    selector_error = "div.MuiDialog-root div.MuiDialogContent-root"

    if page:
        return ejecutar_flujo_en_pagina(
            page,
            pasos_carga,
            nombre_flujo="Cargar pedido",
            selector_exito=selector_exito,
            selector_error=selector_error,
            notificar_estado=notificar_estado,
            espera_resultado_ms=20_000,
        )

    return ejecutar_flujo_playwright(
        pasos_carga,
        nombre_flujo="Cargar pedido",
        selector_exito=selector_exito,
        selector_error=selector_error,
        notificar_estado=notificar_estado,
        headless=headless,
    )


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
