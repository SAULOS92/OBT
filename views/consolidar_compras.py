# views/consolidar_compras.py

import pandas as pd
from io import BytesIO
from datetime import datetime
from flask import (
    Blueprint, render_template, request,
    flash, redirect, url_for, send_file
)
from db import conectar

consolidar_bp = Blueprint(
    "consolidar_compras", __name__,
    template_folder="../templates"
)

# Columnas de entrada y orden exacto
INPUT_COLS = [
    "Pedido",
    "Orden de compra",
    "Cantidad del pedido",
    "Unidad de medida",
    "Codigo Sap Cliente",
    "Factura Sap",
    "Fecha Factura",
    "Material",
    "Descripcion del Material",
    "Cantidad Entrega",
    "Iva_valor",
    "Impuesto Ultraprocesado",
    "Valor_unitario",
    "Tipo Pos",
    "Entrega",
    "Pos.Entrega",
    "Transporte"
]

# Qué campos numéricos consolido
NUMERIC_COLS = [
    "Cantidad del pedido",
    "Cantidad Entrega",
    "Iva_valor",
    "Impuesto Ultraprocesado",
    "Valor_unitario"
]

# Sobre qué campos agrupo en ambos formatos
GROUP_FIELDS = [
    "Unidad de medida",
    "Codigo Sap Cliente",
    "Material",
    "Descripcion del Material",
    "Tipo Pos"
]


@consolidar_bp.route("/consolidar-compras", methods=["GET", "POST"])
def consolidar_compras_index():
    if request.method == "POST":
        fmt = request.form.get("format")  # 'celluweb' o 'ecom'
        f   = request.files.get("archivo")

        if not f or fmt not in ("celluweb", "ecom"):
            flash("Sube un Excel y elige un formato válido.", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # 1) Leo sin tocar nombres
        try:
            df = pd.read_excel(f, engine="openpyxl", dtype=str).fillna("")
        except Exception as e:
            flash(f"Error leyendo el Excel: {e}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # 2) Validación de encabezados
        falt = [c for c in INPUT_COLS if c not in df.columns]
        if falt:
            flash(f"Faltan columnas en el Excel de entrada: {falt}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))
        dup = df.columns[df.columns.duplicated()].unique().tolist()
        if dup:
            flash(f"Encabezados duplicados en el Excel de entrada: {dup}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # 3) Si ECOM, filtro Tipo Pos
        if fmt == "ecom":
            df = df[~df["Tipo Pos"].isin(["ZCMM", "ZCM2"])]

        # 4) Convierto a numérico y agrupo
        for c in NUMERIC_COLS:
            df[c] = df[c].replace("", "0").astype(float)

        agg = (
            df
            .groupby(GROUP_FIELDS, as_index=False)
            .agg({
                "Cantidad del pedido":        "sum",
                "Cantidad Entrega":            "sum",
                "Iva_valor":                   "sum",
                "Impuesto Ultraprocesado":     "sum",
                "Valor_unitario":              "mean"
            })
        )

        # 5) Campos estáticos comunes
        static_vals = {
            "Pedido":        "0",
            "Orden de compra":"23",
            "Factura Sap":   "FC",
            "Fecha Factura": "0",
            "Entrega":       "0",
            "Pos.Entrega":   "0",
            "Transporte":    "0"
        }
        for col, val in static_vals.items():
            agg[col] = val

        # 6) En ECOM calculo Valor_neto y renombro si hace falta
        if fmt == "ecom":
            agg["Valor_neto"] = agg["Valor_unitario"] * agg["Cantidad Entrega"]

        # 7) Elijo columnas y nombre de archivo según formato
        hoy = datetime.now().strftime("%Y%m%d")
        if fmt == "celluweb":
            # **mismo INPUT_COLS**, pero ahora consolidadas
            df_out  = agg[INPUT_COLS]
            filename = f"consolidado_celuweb_{hoy}.xlsx"
        else:
            # Para ECOM incluyo Valor_neto al final
            OUTPUT_COLS_ECOM = INPUT_COLS + ["Valor_neto"]
            df_out  = agg[[c for c in OUTPUT_COLS_ECOM if c in agg.columns]]
            filename = f"consolidado_ecom_{hoy}.xlsx"

        # 8) Generar y enviar Excel
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df_out.to_excel(w, index=False)
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # GET
    return render_template("consolidar_compras.html")




