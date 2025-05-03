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
    # campos adicionales para ECOM
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

@consolidar_bp.route("/consolidar-compras", methods=["GET","POST"])
def consolidar_compras_index():
    if request.method == "POST":
        f = request.files.get("archivo")
        fmt = request.form.get("format")  # 'celluweb' o 'ecom'
        if not f or fmt not in ("celluweb","ecom"):
            flash("Sube un Excel y elige un formato.", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # 1) Leer todo como texto y normalizar nombres
        try:
            df = pd.read_excel(f, engine="openpyxl", dtype=str).fillna("")
            df = normalize_cols(df)
        except Exception as e:
            flash(f"Error leyendo o normalizando: {e}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # 2) Si elegimos ECOM, aplicamos el filtro de Tipo Pos
        if fmt == "ecom":
            df = df[~df["tipo_pos"].isin(["ZCMM", "ZCM2"])]

        # 3) Convertir a numérico los campos que corresponden
        for c in ("cantidad_pedido","cantidad_entrega","iva_valor",
                  "impuesto_ultraprocesado","valor_unitario"):
            if c in df.columns:
                df[c] = df[c].replace("", "0").astype(float)

        # 4) Agrupar por los campos comunes
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

        # 5) Añadir todas las columnas estáticas (incluso las extras para ECOM)
        static_vals = {
            "pedido": "0",
            "orden_de_compra": "23",
            "factura_sap": "FC",
            "fecha_factura": "0",
            "entrega": "0",
            "pos_entrega": "0",
            "transporte": "0",
            # extras ECOM (se usan solo si fmt == "ecom")
            "valor_pedido": "0",
            "vendedor": "0",
            "nombre_cliente": "0",
            "ciudad": "0",
            "nit_compania": "0",
            "codigo_barras_material": "0",
            "categoria": "0",
            "causa_no_despacho": "0"
        }
        for col, val in static_vals.items():
            agg[col] = val

        # 6) Preparar nombre de archivo con fecha
        hoy = datetime.now().strftime("%Y%m%d")

        if fmt == "celluweb":
            # EXACTO orden y nombres de columnas para Celluweb
            out_cols = [
                "pedido",
                "orden_de_compra",
                "cantidad_pedido",
                "unidad_medida",
                "codigo_sap_cliente",
                "factura_sap",
                "fecha_factura",
                "material",
                "descripcion_material",
                "cantidad_entrega",
                "iva_valor",
                "impuesto_ultraprocesado",
                "valor_unitario",
                "tipo_pos",
                "entrega",
                "pos_entrega",
                "transporte"
            ]
            filename = f"consolidado_celluweb_{hoy}.xlsx"
        else:
            # ECOM: antes de exportar calculamos valor_neto
            agg["valor_neto"] = agg["valor_unitario"] * agg["cantidad_entrega"]
            # EXACTO orden y nombres de columnas para ECOM
            out_cols = [
                "pedido",
                "orden_de_compra",
                "cantidad_pedido",
                "unidad_medida",
                "valor_pedido",
                "vendedor",
                "codigo_sap_cliente",
                "nombre_cliente",
                "factura_sap",
                "fecha_factura",
                "ciudad",
                "nit_compania",
                "codigo_barras_material",
                "categoria",
                "material",
                "descripcion_material",
                "causa_no_despacho",
                "cantidad_entrega",
                "unidad_medida",              # reutilizamos para Unidad de medida facturada
                "iva_valor",
                "valor_unitario",
                "valor_neto",
                "tipo_pos"
            ]
            filename = f"consolidado_ecom_{hoy}.xlsx"

        # 7) Generar y enviar el Excel con el orden y nombres exactos
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            agg[out_cols].to_excel(writer, sheet_name="Consolidado", index=False)
        buf.seek(0)

        return send_file(
            buf,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    return render_template("consolidar_compras.html")


