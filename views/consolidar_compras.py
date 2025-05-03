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

# 1) Mapeo de sinónimos → clave interna (snake_case) para leer
COL_MAP = {
    "pedido":                 ["Pedido"],
    "orden_de_compra":        ["Orden de compra"],
    "cantidad_pedido":        ["Cantidad del pedido"],
    "unidad_medida":          ["Unidad de medida"],
    "codigo_sap_cliente":     ["Codigo Sap Cliente"],
    "factura_sap":            ["Factura Sap"],
    "fecha_factura":          ["Fecha Factura"],
    "material":               ["Material"],
    "descripcion_material":   ["Descripcion del Material"],
    "cantidad_entrega":       ["Cantidad Entrega"],
    "iva_valor":              ["Iva_valor"],
    "impuesto_ultraprocesado":["Impuesto Ultraprocesado"],
    "valor_unitario":         ["Valor_unitario"],
    "tipo_pos":               ["Tipo Pos"],
    "entrega":                ["Entrega"],
    "pos_entrega":            ["Pos.Entrega"],
    "transporte":             ["Transporte"],
    # extras para ECOM
    "valor_pedido":           ["Valor pedido","Valor_pedido"],
    "vendedor":               ["Vendedor"],
    "nombre_cliente":         ["Nombre cliente","Nombre_cliente"],
    "ciudad":                 ["Ciudad"],
    "nit_compania":           ["Nit_compania","Nit compañia"],
    "codigo_barras_material": ["Codigo barras material","Codigo_barras_material"],
    "categoria":              ["Categoria"],
    "causa_no_despacho":      ["Causa no despacho","Causa_no_despacho"]
}

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    inv = {}
    for internal, syns in COL_MAP.items():
        for s in syns:
            inv[s.strip().lower()] = internal
    return df.rename(columns={
        c: inv[c.strip().lower()]
        for c in df.columns
        if c.strip().lower() in inv
    })

# 2) Mapas internos → nombres finales EXACTOS
DISPLAY_CELLUWEB = [
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

DISPLAY_ECOM = [
    "Pedido",
    "Orden de compra",
    "Cantidad del pedido",
    "Unidad de medida",
    "Valor pedido",
    "Vendedor",
    "Codigo Sap Cliente",
    "Nombre cliente",
    "Factura Sap",
    "Fecha Factura",
    "Ciudad",
    "Nit_compania",
    "Codigo barras material",
    "Categoria",
    "Material",
    "Descripcion del Material",
    "Causa no despacho",
    "Cantidad Entrega",
    "Unidad de medida",         # para Unidad_de_medida_facturada
    "Iva_valor",
    "Impuesto Ultraprocesado",
    "Valor_unitario",
    "Valor_neto",
    "Tipo Pos"
]

@consolidar_bp.route("/consolidar-compras", methods=["GET","POST"])
def consolidar_compras_index():
    if request.method == "POST":
        archivo = request.files.get("archivo")
        fmt = request.form.get("format")  # 'celluweb' o 'ecom'
        if not archivo or fmt not in ("celluweb","ecom"):
            flash("Sube un Excel y elige un formato.", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        try:
            df = pd.read_excel(archivo, engine="openpyxl", dtype=str).fillna("")
            df = normalize_cols(df)
        except Exception as e:
            flash(f"Error leyendo o normalizando: {e}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # Si ECOM, filtramos ZCMM/ZCM2
        if fmt == "ecom":
            df = df[~df["tipo_pos"].isin(["ZCMM","ZCM2"])]

        # Numericamente:
        for c in ("cantidad_pedido","cantidad_entrega","iva_valor",
                  "impuesto_ultraprocesado","valor_unitario"):
            if c in df.columns:
                df[c] = df[c].replace("", "0").astype(float)

        # Agrupamos
        group_fields = [
            "unidad_medida","codigo_sap_cliente",
            "material","descripcion_material","tipo_pos"
        ]
        agg = df.groupby(group_fields, as_index=False).agg({
            "cantidad_pedido":        "sum",
            "cantidad_entrega":       "sum",
            "iva_valor":              "sum",
            "impuesto_ultraprocesado":"sum",
            "valor_unitario":         "mean"
        })

        # Columnas estáticas
        static = {
            "pedido": "0",
            "orden_de_compra": "23",
            "factura_sap": "FC",
            "fecha_factura": "0",
            "entrega": "0",
            "pos_entrega": "0",
            "transporte": "0"
        }
        if fmt == "ecom":
            static.update({
                "valor_pedido": "0",
                "vendedor": "0",
                "nombre_cliente": "0",
                "ciudad": "0",
                "nit_compania": "0",
                "codigo_barras_material": "0",
                "categoria": "0",
                "causa_no_despacho": "0"
            })
            # calculamos valor_neto
            agg["valor_neto"] = agg["valor_unitario"] * agg["cantidad_entrega"]

        for col, val in static.items():
            agg[col] = val

        # Armamos la salida con nombres EXACTOS
        hoy = datetime.now().strftime("%Y%m%d")
        if fmt == "celluweb":
            out_display = DISPLAY_CELLUWEB
            filename = f"consolidado_celluweb_{hoy}.xlsx"
            # internal cols en ese orden:
            internal_cols = [
                "pedido","orden_de_compra","cantidad_pedido","unidad_medida",
                "codigo_sap_cliente","factura_sap","fecha_factura","material",
                "descripcion_material","cantidad_entrega","iva_valor",
                "impuesto_ultraprocesado","valor_unitario","tipo_pos",
                "entrega","pos_entrega","transporte"
            ]
        else:
            out_display = DISPLAY_ECOM
            filename = f"consolidado_ecom_{hoy}.xlsx"
            internal_cols = [
                "pedido","orden_de_compra","cantidad_pedido","unidad_medida",
                "valor_pedido","vendedor","codigo_sap_cliente","nombre_cliente",
                "factura_sap","fecha_factura","ciudad","nit_compania",
                "codigo_barras_material","categoria","material",
                "descripcion_material","causa_no_despacho","cantidad_entrega",
                "unidad_medida","iva_valor","impuesto_ultraprocesado",
                "valor_unitario","valor_neto","tipo_pos"
            ]

        # Renombrar internos → display exactos
        rename_map = dict(zip(internal_cols, out_display))
        df_out = agg[internal_cols].rename(columns=rename_map)

        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df_out.to_excel(writer, sheet_name="Consolidado", index=False)
        buffer.seek(0)

        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    return render_template("consolidar_compras.html")


