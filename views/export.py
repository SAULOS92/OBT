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
    """Muestra la página con las opciones de exportación."""
    return render_template("export.html")


@export_bp.route("/resumen", methods=["GET"])
def export_resumen():
    """
    Descarga un Excel con el resultado de fn_obtener_resumen_pedidos(),
    ahora incluyendo la columna 'ruta'.
    """
    # 1) Definimos los encabezados que devuelve la función
    cols = [
        "codigo_cli",
        "nombre",
        "barrio",
        "ciudad",
        "asesor",
        "total_pedidos",
        "ruta"
    ]

    # 2) Intentamos obtener datos de la función en la BD
    try:
        conn = conectar()
        cur = conn.cursor()
        cur.execute("SELECT fn_obtener_resumen_pedidos();")
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row and row[0]:
            data = json.loads(row[0])
        else:
            data = []
    except Exception:
        # Si hay error, devolvemos un Excel solo con encabezados
        data = []

    # 3) Construimos DataFrame (vacío o con filas)
    df = pd.DataFrame(data, columns=cols)

    # 4) Generamos el Excel en memoria
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(
            writer,
            sheet_name="ResumenPedidos",
            index=False
        )
    output.seek(0)

    # 5) Enviamos el archivo
    return send_file(
        output,
        as_attachment=True,
        download_name="resumen_pedidos.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@export_bp.route("/residuos", methods=["POST"])
def export_residuos():
    """
    Lee un Excel de particiones, llama a fn_obtener_residuos
    y devuelve un Excel (sin cambios respecto a antes).
    """
    cols = ["numero_pedido", "codigo_pro", "residuo"]

    # Lectura y validación del Excel de particiones
    f = request.files.get("particiones")
    if not f:
        flash("Sube el Excel de particiones.", "error")
        return redirect(url_for("export.export_index"))

    try:
        df_parts = pd.read_excel(f, engine="openpyxl")
        EXPECTED = ["codigo_pro", "particiones"]
        if list(df_parts.columns) != EXPECTED:
            raise ValueError(f"Encabezados deben ser {EXPECTED}")
        prod_parts = df_parts.to_dict(orient="records")
    except Exception as e:
        flash(f"Error leyendo Excel de particiones: {e}", "error")
        prod_parts = None

    # Llamada al SP y parseo del JSON
    try:
        if prod_parts is not None:
            conn = conectar()
            cur = conn.cursor()
            cur.execute(
                "SELECT fn_obtener_residuos(%s);",
                (json.dumps(prod_parts),)
            )
            row = cur.fetchone()
            cur.close()
            conn.close()
            data = json.loads(row[0]) if (row and row[0]) else []
        else:
            data = []
    except Exception:
        data = []

    # Construcción del DataFrame y generación del Excel
    df_out = pd.DataFrame(data, columns=cols)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_out.to_excel(
            writer,
            sheet_name="Residuos",
            index=False
        )
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name="residuos_pedidos.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
