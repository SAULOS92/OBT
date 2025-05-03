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

# Las 17 columnas que debe tener el Excel de entrada, en este orden
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

# Campos numéricos que luego consolidas
NUMERIC_COLS = [
    "Cantidad del pedido",
    "Cantidad Entrega",
    "Iva_valor",
    "Impuesto Ultraprocesado",
    "Valor_unitario"
]

# Campos por los que agrupas en ECOM
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
        f = request.files.get("archivo")

        # Validar que subieron un archivo y eligieron formato
        if not f or fmt not in ("celluweb", "ecom"):
            flash("Sube un Excel y elige un formato válido.", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # 1) Leer el Excel sin transformar nombres
        try:
            df = pd.read_excel(f, engine="openpyxl", dtype=str)
        except Exception as e:
            flash(f"Error leyendo el Excel: {e}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # 2) Validar encabezados faltantes
        faltantes = [c for c in INPUT_COLS if c not in df.columns]
        if faltantes:
            flash(f"Faltan columnas en el Excel de entrada: {faltantes}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # 3) Detectar encabezados duplicados
        duplicados = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicados:
            flash(f"Encabezados duplicados en el Excel de entrada: {duplicados}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # A partir de aquí, ya puedes seguir con tu lógica:
        # - Si fmt == "celluweb": devolver df[INPUT_COLS] tal cual
        # - Si fmt == "ecom": filtrar "Tipo Pos", agrupar GROUP_FIELDS,
        #   sumar/mean NUMERIC_COLS, asignar valores estáticos y montar el Excel.

        if fmt == "celluweb":
            df_out = df[INPUT_COLS]
            filename = f"consolidado_celuweb_{datetime.now().strftime('%Y%m%d')}.xlsx"
        else:
            # ECOM: filtrado
            df_e = df[~df["Tipo Pos"].isin(["ZCMM", "ZCM2"])].copy()
            # convertir numéricos
            for c in NUMERIC_COLS:
                df_e[c] = df_e[c].replace("", "0").astype(float)
            # agrupar
            agg = df_e.groupby(GROUP_FIELDS, as_index=False)[NUMERIC_COLS].agg({
                "Cantidad del pedido": "sum",
                "Cantidad Entrega": "sum",
                "Iva_valor": "sum",
                "Impuesto Ultraprocesado": "sum",
                "Valor_unitario": "mean"
            })
            # campos estáticos
            static_vals = {
                "Pedido": "0",
                "Orden de compra": "23",
                "Factura Sap": "FC",
                "Fecha Factura": "0",
                "Entrega": "0",
                "Pos.Entrega": "0",
                "Transporte": "0"
            }
            for col, val in static_vals.items():
                agg[col] = val
            # valor neto
            agg["Valor_neto"] = agg["Valor_unitario"] * agg["Cantidad Entrega"]

            # columnas de salida EXACTAS para ECOM (en este orden):
            OUTPUT_COLS_ECOM = [
                "Pedido",
                "Orden de compra",
                "Cantidad del pedido",
                "Unidad de medida",
                "Valor_pedido",            # si tuvieras esta columna o agregarla estática
                "Vendedor",               # idem
                "Codigo Sap Cliente",
                "Nombre_cliente",         # idem
                "Factura Sap",
                "Fecha Factura",
                "Ciudad",                 # idem
                "Nit_compania",           # idem
                "Codigo_barras_material", # idem
                "Categoria",              # idem
                "Material",
                "Descripcion del Material",
                "Causa_no_despacho",      # idem
                "Cantidad Entrega",
                "Unidad_de_medida_facturada", # idem
                "Iva",                    # idem
                "Iva_valor",
                "Valor_unitario",
                "Valor_neto",
                "Tipo de pedido"          # o "Tipo Pos" según necesites
            ]
            # Filtrar solo las que realmente existan en agg
            df_out = agg[[c for c in OUTPUT_COLS_ECOM if c in agg.columns]]
            filename = f"consolidado_ecom_{datetime.now().strftime('%Y%m%d')}.xlsx"

        # 4) Generar y enviar el Excel
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df_out.to_excel(writer, index=False)
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # GET
    return render_template("consolidar_compras.html")



