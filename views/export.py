import json
from io import BytesIO

import pandas as pd
from flask import (
    Blueprint, render_template, request,
    flash, redirect, url_for, send_file
)
from db import conectar

export_bp = Blueprint(
    "export", __name__,
    url_prefix="/export",
    template_folder="../templates"
)

@export_bp.route("/", methods=["GET"])
def export_index():
    return render_template("export.html")


@export_bp.route("/resumen", methods=["GET"])
def export_resumen():
    try:
        conn = conectar()
        cur = conn.cursor()
        cur.execute("SELECT fn_obtener_resumen_pedidos();")
        json_res = cur.fetchone()[0]
    finally:
        cur.close()
        conn.close()

    df = pd.DataFrame(json.loads(json_res))
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="Resumen", index=False)
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name="resumen_pedidos.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@export_bp.route("/residuos", methods=["POST"])
def export_residuos():
    f = request.files.get("particiones")
    EXPECTED = ["codigo_pro", "particiones"]
    if not f:
        flash("Sube el Excel de particiones.", "error")
        return redirect(url_for("export.export_index"))

    try:
        df = pd.read_excel(f, engine="openpyxl")
        if list(df.columns) != EXPECTED:
            raise ValueError(f"Encabezados deben ser {EXPECTED}")
    except Exception as e:
        flash(f"Error leyendo Excel: {e}", "error")
        return redirect(url_for("export.export_index"))

    prod_parts = df.to_dict(orient="records")
    try:
        conn = conectar()
        cur = conn.cursor()
        cur.execute(
            "SELECT fn_obtener_residuos(%s);",
            (json.dumps(prod_parts),)
        )
        json_res = cur.fetchone()[0]
    finally:
        cur.close()
        conn.close()

    df_out = pd.DataFrame(json.loads(json_res))
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as w:
        df_out.to_excel(w, sheet_name="Residuos", index=False)
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name="residuos_pedidos.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
