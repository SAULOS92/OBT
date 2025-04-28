import json
import pandas as pd
from flask import (
    Blueprint, render_template, request,
    flash, redirect, url_for
)
from db import conectar

upload_bp = Blueprint(
    "upload", __name__,
    template_folder="../templates"
)

PED_HEADERS = [
  "numero_pedido","hora","cliente","nombre","barrio","ciudad",
  "asesor","codigo_pro","producto","cantidad","valor","tipo","estado"
]
RUT_HEADERS = ["cliente","dia","codigo_ruta"]
DIAS_VALIDOS = {"LU","MA","MI","JU","VI","SA","DO"}

@upload_bp.route("/", methods=["GET","POST"])
def upload_index():
    if request.method == "POST":
        f_ped = request.files.get("pedidos")
        f_rut = request.files.get("rutas")
        p_dia = request.form.get("dia","").strip()

        # 1) Validar día
        if p_dia not in DIAS_VALIDOS:
            flash(f"Día inválido. Elige uno de: {', '.join(DIAS_VALIDOS)}", "error")
            return redirect(url_for("upload.upload_index"))

        # 2) Leer Excel
        try:
            df_ped = pd.read_excel(f_ped, engine="openpyxl")
            df_rut = pd.read_excel(f_rut, engine="openpyxl")
        except Exception as e:
            flash(f"Error leyendo Excel: {e}", "error")
            return redirect(url_for("upload.upload_index"))

        # 3) Validar encabezados
        if list(df_ped.columns) != PED_HEADERS:
            flash(f"Encabezados de pedidos inválidos, deben ser: {PED_HEADERS}", "error")
            return redirect(url_for("upload.upload_index"))
        if list(df_rut.columns) != RUT_HEADERS:
            flash(f"Encabezados de rutas inválidos, deben ser: {RUT_HEADERS}", "error")
            return redirect(url_for("upload.upload_index"))

        # 4) A JSON y llamada al SP
        pedidos = df_ped.to_dict(orient="records")
        rutas   = df_rut.to_dict(orient="records")
        try:
            conn = conectar()
            cur = conn.cursor()
            cur.execute(
                "CALL etl_cargar_pedidos_y_rutas_masivo(%s,%s,%s);",
                (json.dumps(pedidos), json.dumps(rutas), p_dia)
            )
            conn.commit()
            flash("¡Carga masiva exitosa!", "success")
        except Exception as e:
            flash(f"Error en ETL: {e}", "error")
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("upload.upload_index"))

    return render_template("upload.html")
