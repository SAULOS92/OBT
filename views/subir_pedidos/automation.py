"""Funciones de automatización basadas en Playwright."""

import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
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


def construir_flujo_cargar_pedido(ruta_archivo: str) -> Dict[str, Any]:
    """Arma el flujo y selectores para cargar un pedido masivo.

    La estructura se separa en un helper reutilizable para mantener el flujo
    de pasos agrupado y facilitar futuras modificaciones.
    """

    pasos_carga = [
        {
            "nombre": "Ir a la pagina de carga masiva",
            "tipo": "navegar",
            "valor": "https://portal.gruponutresa.com/p/nuevo/pedido-masivo/excel",
        },
        {
            "nombre": "Seleccionar tipo plantilla",
            "tipo": "campo de seleccion",
            "selector": (
                "#root > div > section > article > section > section > form > "
                "div > fieldset > article > div > div"
            ),
            "script": """
(() => {
  const trigger = document.querySelector("#root > div > section > article > section > section > form > div > fieldset > article > div > div");
  if (!trigger) return console.log("❌ combo no encontrado");
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
})();
            """,
        },
        {
            "nombre": "Seleccionar archivo",
            "tipo": "archivo",
            "selector": (
                "#root > div > section > article > section > section > form > "
                "section > label > section.file-input__upload"
            ),
            "valor": ruta_archivo,
            "script": """
(() => {
  // busca el input de tipo file dentro del form
  const input = document.querySelector("#root form input[type='file']");
  if (!input) return console.log("❌ no encontré el input file");

  // habilitarlo por si está oculto/deshabilitado
  input.removeAttribute("disabled");
  input.style.display = "block";
  input.style.visibility = "visible";
  input.style.opacity = 1;

  // forzar click → abre el diálogo de archivos
  input.click();
  console.log("✅ click en input[file], debería abrir el diálogo");
})();
            """,
        },
        {
            "nombre": "enviar archivo",
            "tipo": "click",
            "selector": (
                "#root > div > section > article > section > section > form > footer "
                "> button.MuiButtonBase-root.MuiButton-root.MuiButton-text.MuiButton-textPrimary."
                "MuiButton-sizeMedium.MuiButton-textSizeMedium.MuiButton-root."
                "MuiButton-text.MuiButton-textPrimary.MuiButton-sizeMedium."
                "MuiButton-textSizeMedium.mb2.admin__create-footer-save.w5-ns.css-kp69xf"
            ),
        },
    ]

    selector_exito = (
        "#root > div > section > article > section > article > footer > button:nth-child(3)"
    )
    selector_error = (
        "body > div.MuiDialog-root.MuiModal-root.css-126xj0f > "
        "div.MuiDialog-container.MuiDialog-scrollPaper.css-ekeie0 > div "
        "> div.MuiDialogContent-root.css-1ty026z"
    )

    return {
        "pasos": pasos_carga,
        "selector_exito": selector_exito,
        "selector_error": selector_error,
    }


def cargar_pedido_masivo_excel(
    ruta_archivo: str,
    *,
    notificar_estado: Optional[Callable[[str], None]] = None,
    headless: bool = True,
    page=None,
) -> bool:
    """Carga un archivo de pedido masivo en el portal Grupo Nutresa."""

    flujo = construir_flujo_cargar_pedido(ruta_archivo)
    pasos_carga = flujo["pasos"]
    selector_exito = flujo["selector_exito"]
    selector_error = flujo["selector_error"]

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


def crear_archivo_pedido_masivo(
    destino: str,
    filas: Optional[List[Dict[str, Any]]] = None,
    filas_en_blanco: int = 3,
) -> str:
    """Genera un Excel con el formato esperado para cargar pedidos masivos.

    Se generan las columnas visibles en el portal (``codigo_pro``,
    ``producto``, ``UN`` y ``pedir``) con datos de ejemplo similares a la
    plantilla de la imagen proporcionada, facilitando la carga automatizada.

    El archivo respeta que las primeras filas estén vacías (por defecto tres
    filas), dejando el encabezado en la cuarta fila tal como se observa en la
    plantilla de referencia.

    Args:
        destino: Ruta completa donde se guardará el archivo .xlsx.
        filas: Datos opcionales a incluir. Cada fila debe tener las claves
            ``codigo_pro``, ``producto``, ``UN`` y ``pedir``. Si no se
            proporcionan, se usan dos registros de ejemplo.

    Returns:
        Ruta final (string) del archivo generado.
    """

    if filas is None:
        filas = [
            {
                "codigo_pro": 1015235,
                "producto": "2 SALCH. SP. RANCHERA X 120G",
                "UN": "UN",
                "pedir": 5,
            },
            {
                "codigo_pro": 1075657,
                "producto": "CHORIZO RICA X 175 G",
                "UN": "UN",
                "pedir": 5,
            },
        ]

    df = pd.DataFrame(filas, columns=["codigo_pro", "producto", "UN", "pedir"])
    ruta_destino = Path(destino)
    ruta_destino.parent.mkdir(parents=True, exist_ok=True)

    start_row = max(0, int(filas_en_blanco))
    with pd.ExcelWriter(ruta_destino, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, startrow=start_row)

    return str(ruta_destino)


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
