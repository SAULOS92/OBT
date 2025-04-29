# views/export.py

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
    cols = ["codigo_cli", "nombre", "barrio", "ciudad", "asesor", "total_pedidos", "ruta"]

    # 1) Llamada a la BD
    try:
        conn = conectar()
        cur = conn.cursor()
        cur.execute("SELECT fn_obtener_resumen_pedidos();")
        row = cur.fetchone()
        cur.close()
        conn.close()
    except Exception as e:
        flash(f"Error al consultar el resumen en la base de datos: {e}", "error")
        return redirect(url_for("export.export_index"))

    # 2) Interpretar resultado
    raw = row[0] if row else None
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except Exception as e:
            flash(f"Error al parsear los datos del resumen: {e}", "error")
            return redirect(url_for("export.export_index"))
    elif isinstance(raw, (list, dict)):
        data = raw
    else:
        data = []

    # 3) DataFrame
    df = pd.DataFrame(data, columns=cols)

    # 4) Generar Excel
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="ResumenPedidos", index=False)
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name="resumen_pedidos.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@export_bp.route("/residuos", methods=["POST"])
def export_residuos():
    cols = ["numero_pedido", "codigo_pro", "residuo"]

    f = request.files.get("particiones")
    if not f:
        flash("Debes subir el Excel de particiones.", "error")
        return redirect(url_for("export.export_index"))

    try:
        df_parts = pd.read_excel(f, engine="openpyxl")
    except Exception as e:
        flash(f"Error leyendo el Excel de particiones: {e}", "error")
        return redirect(url_for("export.export_index"))

    EXPECTED = ["codigo_pro", "particiones"]
    if list(df_parts.columns) != EXPECTED:
        flash(f"Encabezados inválidos en particiones. Deben ser: {EXPECTED}", "error")
        return redirect(url_for("export.export_index"))

    prod_parts = df_parts.to_dict(orient="records")

    # 1) Llamada a la BD
    try:
        conn = conectar()
        cur = conn.cursor()
        cur.execute("SELECT fn_obtener_residuos(%s);", (json.dumps(prod_parts),))
        row = cur.fetchone()
        cur.close()
        conn.close()
    except Exception as e:
        flash(f"Error al consultar los residuos en la base de datos: {e}", "error")
        return redirect(url_for("export.export_index"))

    # 2) Interpretar resultado
    raw = row[0] if row else None
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except Exception as e:
            flash(f"Error al parsear los datos de residuos: {e}", "error")
            return redirect(url_for("export.export_index"))
    elif isinstance(raw, (list, dict)):
        data = raw
    else:
        data = []

    # 3) DataFrame
    df_out = pd.DataFrame(data, columns=cols)

    # 4) Generar Excel
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_out.to_excel(writer, sheet_name="Residuos", index=False)
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name="residuos_pedidos.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

