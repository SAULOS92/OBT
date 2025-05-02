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

# Mapeo de sinónimos a nombres internos
COL_MAP = {
    "pedido":                 ["Pedido"],
    "orden_de_compra":        ["Orden de compra"],
    "cantidad_pedido":        ["Cantidad del pedido"],
    "unidad_medida":          ["Unidad de medida"],
    "valor_pedido":           ["Valor_pedido", "Valor pedido"],
    "vendedor":               ["Vendedor"],
    "codigo_sap_cliente":     ["Codigo Sap Cliente"],
    "nombre_cliente":         ["Nombre_cliente","Nombre cliente"],
    "factura_sap":            ["Factura Sap","Factura_sap"],
    "fecha_factura":          ["Fecha Factura"],
    "ciudad":                 ["Ciudad"],
    "nit_compania":           ["Nit_compania","Nit_compañia"],
    "codigo_barras_material": ["Codigo_barras_material"],
    "categoria":              ["Categoria"],
    "material":               ["Material"],
    "descripcion_material":   ["Descripcion del Material"],
    "causa_no_despacho":      ["Causa_no_despacho"],
    "unidad_medida_facturada":["Unidad_de_medida_facturada"],
    "cantidad_entrega":       ["Cantidad Entrega"],
    "cantidad_facturada":     ["Cantidad_facturada"],
    "iva":                    ["Iva","IVA"],
    "iva_valor":              ["Iva_valor"],
    "impuesto_ultraprocesado":["Impuesto Ultraprocesado"],
    "valor_unitario":         ["Valor_unitario"],
    "valor_neto":             ["Valor_neto"],
    "tipo_pos":               ["Tipo Pos"],
    "transporte":             ["Transporte"]
}

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    inv = {}
    for internal, syns in COL_MAP.items():
        for s in syns:
            inv[s.strip().lower()] = internal
    return df.rename(columns={c: inv[c.strip().lower()]
                               for c in df.columns
                               if c.strip().lower() in inv})

@consolidar_bp.route("/consolidar-compras", methods=["GET","POST"])
def consolidar_compras_index():
    if request.method == "POST":
        f = request.files.get("archivo")
        fmt = request.form.get("format")  # 'celluweb' o 'ecom'
        if not f or fmt not in ("celluweb","ecom"):
            flash("Sube un Excel y elige un formato.", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        try:
            df = pd.read_excel(f, engine="openpyxl", dtype=str).fillna("")
            df = normalize_cols(df)
            # filtrar tipo_pos
            df = df[~df["tipo_pos"].isin(["ZCMM","ZCM2"])]
        except Exception as e:
            flash(f"Error leyendo o normalizando: {e}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # columnas numéricas a convertir
        num_cols = [
            "cantidad_pedido","cantidad_entrega","cantidad_facturada",
            "iva_valor","impuesto_ultraprocesado","valor_unitario","valor_neto"
        ]
        for c in num_cols:
            if c in df.columns:
                df[c] = df[c].replace("","0").astype(float)

        # columnas de agrupación comunes (la unión de ambos formatos)
        common_group = [
            "pedido","orden_de_compra","cantidad_pedido","unidad_medida",
            "valor_pedido","vendedor","codigo_sap_cliente","nombre_cliente",
            "factura_sap","fecha_factura","ciudad","nit_compania",
            "codigo_barras_material","categoria","material",
            "descripcion_material","causa_no_despacho",
            "unidad_medida_facturada","iva","iva_valor","tipo_pos","transporte"
        ]
        group_by = [c for c in common_group if c in df.columns]

        # agregaciones: sum of all except valor_unitario (mean)
        aggs = {c:"sum" for c in num_cols if c!="valor_unitario"}
        if "valor_unitario" in df.columns:
            aggs["valor_unitario"] = "mean"

        try:
            agg = df.groupby(group_by, as_index=False).agg(aggs)
        except Exception as e:
            flash(f"Error al consolidar datos: {e}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # prepara nombre de archivo
        hoy = datetime.now().strftime("%Y%m%d")

        if fmt=="celluweb":
            # columnas y orden para Celluweb
            columnas = [
                "orden_de_compra","unidad_medida","codigo_sap_cliente",
                "factura_sap","fecha_factura","material",
                "descripcion_material","transporte","valor_unitario",
                "cantidad_pedido","cantidad_entrega",
                "iva_valor","impuesto_ultraprocesado"
            ]
            df_out = agg[[c for c in columnas if c in agg.columns]]
            filename = f"consolidado_celluweb_{hoy}.xlsx"

        else:  # ecom
            # columnas y orden según tu SELECT
            columnas = [
                "pedido","orden_de_compra","cantidad_pedido","unidad_medida",
                "valor_pedido","vendedor","codigo_sap_cliente","nombre_cliente",
                "factura_sap","fecha_factura","ciudad","nit_compania",
                "codigo_barras_material","categoria","material",
                "descripcion_material","causa_no_despacho","cantidad_facturada",
                "unidad_medida_facturada","iva","iva_valor",
                "valor_unitario","valor_neto","tipo_pos"
            ]
            df_out = agg[[c for c in columnas if c in agg.columns]]
            # renombrar tipo_pos → Tipo de pedido
            if "tipo_pos" in df_out.columns:
                df_out = df_out.rename(columns={"tipo_pos":"Tipo de pedido"})
            filename = f"consolidado_ecom_{hoy}.xlsx"

        # generar y enviar Excel
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df_out.to_excel(writer, sheet_name="Consolidado", index=False)
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    return render_template("consolidar_compras.html")
