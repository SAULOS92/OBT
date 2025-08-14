import json
import pathlib

import pandas as pd
from flask import Blueprint, render_template, request, session
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from db import conectar
from views.auth import login_required


subir_pedidos_bp = Blueprint("subir_pedidos", __name__, template_folder="../templates")


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
    "carga.fileInput": "form input[type='file']",
    "carga.guardar": "#root > div > section > article > section > section > form > footer > button.MuiButtonBase-root.MuiButton-root.MuiButton-text.MuiButton-textPrimary.MuiButton-sizeMedium.MuiButton-textSizeMedium.MuiButton-root.MuiButton-text.MuiButton-textPrimary.MuiButton-sizeMedium.MuiButton-textSizeMedium.mb2.admin__create-footer-save.w5-ns.css-kp69xf",
    "carga.continuar": "#root > div > section > article > section > article > footer > button:nth-child(3) > span.MuiButton-startIcon.MuiButton-iconSizeMedium.css-6xugel > svg",
    "canal.input": "#purchaseOrderNN13CANALT",
    "canal.value": "14",
    "placa.input": "#formValue",
    "carrito.confirmar": "#root > div > section > article > section > section > section.cart__resume-options > button:nth-child(3)",
    "respuesta.aceptar": "#root > div > section > article > section > section > section > section.order__confirmation-products > article.order__confirmation-products-button > button",
}


def _crear_excel(bd: str, ruta: int) -> pathlib.Path:
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT fn_obtener_pedidos_con_pedir_json(%s);", (bd,))
            pedidos = json.loads(cur.fetchone()[0] or "[]")
    registros = [p for p in pedidos if p["ruta"] == ruta]
    if not registros:
        raise ValueError(f"Sin pedidos para ruta {ruta}")
    df = pd.DataFrame(registros)[["codigo_pro", "producto", "pedir"]]
    df.insert(2, "UN", "UN")
    path = pathlib.Path(f"{ruta}.xlsx")
    df.to_excel(path, sheet_name="Pedidos", startrow=4, index=False, engine="openpyxl")
    return path


def _build_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    service = Service()
    return webdriver.Chrome(options=options, service=service)


def _login(driver, usuario: str, password: str) -> None:
    driver.get(CONFIG["login.url"])
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, CONFIG["login.user"]))
    ).send_keys(usuario)
    driver.find_element(By.CSS_SELECTOR, CONFIG["login.pass"]).send_keys(password)
    driver.find_element(By.CSS_SELECTOR, CONFIG["login.submit"]).click()
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, CONFIG["login.ok"]))
    )


def _ingresar_modulo(driver) -> None:
    driver.get(CONFIG["modulo.url"])
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, CONFIG["modulo.ok"]))
    )


def _cargar_archivo(driver, xls: pathlib.Path) -> None:
    try:
        driver.find_element(By.CSS_SELECTOR, CONFIG["carga.combo1"]).click()
        driver.find_element(By.CSS_SELECTOR, CONFIG["carga.combo2"]).click()
    except Exception:
        pass
    driver.find_element(By.CSS_SELECTOR, CONFIG["carga.fileInput"]).send_keys(str(xls))
    driver.find_element(By.CSS_SELECTOR, CONFIG["carga.guardar"]).click()
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, CONFIG["carga.continuar"]))
    ).click()


def _agregar_carrito(driver, placa: str) -> None:
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, CONFIG["canal.input"]))
    ).send_keys(CONFIG["canal.value"])
    if placa:
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, CONFIG["placa.input"]))
            ).send_keys(placa)
        except Exception:
            driver.get("https://portal.gruponutresa.com/carrito/resumen")
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, CONFIG["placa.input"]))
            ).send_keys(placa)


def _aceptar_pedido(driver) -> None:
    driver.get("https://portal.gruponutresa.com/carrito/resumen")


def _confirmar_pedido(driver) -> None:
    WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, CONFIG["carrito.confirmar"]))
    ).click()
    try:
        WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, CONFIG["respuesta.aceptar"]))
        ).click()
    except Exception:
        pass


def _ejecutar(bd: str, ruta: int, usuario: str, password: str, placa: str) -> None:
    xls = _crear_excel(bd, ruta)
    driver = _build_driver()
    try:
        _login(driver, usuario, password)
        _ingresar_modulo(driver)
        _cargar_archivo(driver, xls)
        _agregar_carrito(driver, placa)
        _aceptar_pedido(driver)
        _confirmar_pedido(driver)
    finally:
        driver.quit()
        if xls.exists():
            xls.unlink()


@subir_pedidos_bp.route("/subir-pedidos", methods=["GET", "POST"])
@login_required
def subir_pedidos():
    mensaje = ""
    if request.method == "POST":
        usuario = request.form["usuario"]
        password = request.form["contrasena"]
        ruta = int(request.form["ruta"])
        placa = request.form.get("placa", "")
        try:
            bd = session.get("empresa")
            _ejecutar(bd, ruta, usuario, password, placa)
            mensaje = "Pedido enviado correctamente."
        except Exception as e:
            mensaje = f"Error: {e}"
    return render_template("subir_pedidos.html", mensaje=mensaje)

