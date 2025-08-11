# views/subir_pedidos.py

import os
import json
import pandas as pd
from flask import (
    Blueprint,
    session,
    flash,
    redirect,
    url_for,
    render_template,
    request,
)

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from db import conectar
from views.auth import login_required


subir_pedidos_bp = Blueprint(
    "subir_pedidos", __name__, template_folder="../templates"
)


@subir_pedidos_bp.route("/subir-pedidos", methods=["GET", "POST"])
@login_required
def subir_pedidos():
    if request.method == "GET":
        vehiculos = []
        conn = cur = None
        try:
            conn = conectar()
            cur = conn.cursor()
            cur.execute("SELECT id, placa, carro FROM vehiculos ORDER BY id")
            vehiculos = [
                {"id": r[0], "placa": r[1], "carro": r[2]} for r in cur.fetchall()
            ]
        except Exception as e:
            flash(f"❌ Error cargando vehículos: {e}", "danger")
        finally:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        return render_template("subir_pedidos.html", vehiculos=vehiculos)

    try:
        empresa = session.get("empresa")
        if not empresa:
            flash("❌ Falta empresa en sesión", "danger")
            return redirect(url_for("subir_pedidos.subir_pedidos"))

        ruta = request.form["ruta"]
        placa = request.form["placa"]
        usuario = request.form["usuario"]
        password = request.form["password"]

        conn = conectar()
        cur = conn.cursor()
        cur.execute("SELECT fn_obtener_pedidos_con_pedir_json(%s);", (empresa,))
        pedidos = json.loads(cur.fetchone()[0])
        cur.close()
        conn.close()

        if not pedidos:
            flash(f'❌ No hay pedidos para "{empresa}"', "danger")
            return redirect(url_for("subir_pedidos.subir_pedidos"))

        primera_ruta = next(iter({p["ruta"] for p in pedidos}))
        registros = [p for p in pedidos if p["ruta"] == primera_ruta]

        df = pd.DataFrame(registros)[["codigo_pro", "producto", "pedir"]]
        df.insert(2, "UN", "UN")
        fichero = f"{primera_ruta}.xlsx"
        df.to_excel(
            fichero,
            sheet_name="Pedidos",
            startrow=4,
            index=False,
            engine="openpyxl",
        )

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(options=options)

        driver.get("https://portal.gruponutresa.com/")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.NAME, "username"))
        ).send_keys(usuario)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.NAME, "password"))
        ).send_keys(password)
        driver.find_element(By.CSS_SELECTOR, "form").submit()

        driver.get(
            "https://portal.gruponutresa.com/p/nuevo/pedido-masivo/excel"
        )
        file_input = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="file"]'))
        )
        file_input.send_keys(os.path.abspath(fichero))
        driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()

        try:
            btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[contains(., 'Agregar al carrito')]")
                )
            )
            btn.click()
        except Exception:
            pass

        driver.get("https://portal.gruponutresa.com/carrito/resumen")
        try:
            WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[contains(., 'Confirmar pedido')]")
                )
            ).click()
        except Exception:
            pass

        driver.quit()
        os.remove(fichero)
        flash("✅ Pedido ruta1 subido y confirmado", "success")
    except Exception as e:
        flash(f"❌ Error: {e}", "danger")

    return redirect(url_for("subir_pedidos.subir_pedidos"))


@subir_pedidos_bp.route("/subir-pedidos/vehiculos/<int:vid>", methods=["POST"])
@login_required
def actualizar_vehiculo(vid):
    data = request.get_json()
    placa = data.get("placa")
    carro = data.get("carro")
    conn = conectar()
    cur = conn.cursor()
    cur.execute(
        "UPDATE vehiculos SET placa=%s, carro=%s WHERE id=%s",
        (placa, carro, vid),
    )
    conn.commit()
    cur.close()
    conn.close()
    return ("", 204)
