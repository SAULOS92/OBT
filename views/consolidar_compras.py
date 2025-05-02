import pandas as pd
from io import BytesIO
from flask import (
    Blueprint, render_template, request,
    flash, redirect, url_for, send_file
)
from db import conectar

consolidar_bp = Blueprint(
    "consolidar_compras", __name__,
    template_folder="../templates"
)

# Mapeo para normalizar
COL_MAP = {
    "pedido":               ["Pedido"],
    "orden_de_compra":      ["Orden de compra"],
    "cantidad_pedido":      ["Cantidad del pedido"],
    "unidad_medida":        ["Unidad de medida"],
    "valor_pedido":         ["Valor_pedido","Valor pedido"],
    "vendedor":             ["Vendedor"],
    "codigo_sap_cliente":   ["Codigo Sap Cliente"],
    "nombre_cliente":       ["Nombre_cliente","Nombre cliente"],
    "factura_sap":          ["Factura Sap","Factura_sap"],
    "fecha_factura":        ["Fecha Factura"],
    "ciudad":               ["Ciudad"],
    "nit_compania":         ["Nit_compania","Nit_compañia"],
    "codigo_barras_material":["Codigo_barras_material"],
    "categoria":            ["Categoria"],
    "material":             ["Material"],
    "descripcion_material": ["Descripcion del Material"],
    "causa_no_despacho":    ["Causa_no_despacho"],
    "cantidad_facturada":   ["Cantidad_facturada"],
    "unidad_medida_facturada":["Unidad_de_medida_facturada"],
    "iva":                  ["Iva","IVA"],
    "iva_valor":            ["Iva_valor"],
    "valor_unitario":       ["Valor_unitario"],
    "valor_neto":           ["Valor_neto"],
    "tipo_pos":             ["Tipo Pos"]
}

def normalize_cols(df):
    inv = {}
    for k, syns in COL_MAP.items():
        for s in syns:
            inv[s.strip().lower()] = k
    return df.rename(columns={c:inv[c.strip().lower()] for c in df.columns if c.strip().lower() in inv})


@consolidar_bp.route("/consolidar-compras", methods=["GET","POST"])
def consolidar_compras_index():
    if request.method=="POST":
        f = request.files.get("archivo")
        fmt = request.form.get("format")  # 'celluweb' o 'ecom'
        if not f or fmt not in ("celluweb","ecom"):
            flash("Sube un Excel y elige un formato.", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        try:
            # 1) Leer y normalizar
            df = pd.read_excel(f, engine="openpyxl", dtype=str).fillna("")
            df = normalize_cols(df)

            # 2) Filtrar Tipo Pos
            df = df[~df["tipo_pos"].isin(["ZCMM","ZCM2"])]

        except Exception as e:
            flash(f"Error leyendo/normalizando: {e}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        if fmt=="celluweb":
            # ---- CELLUWEB: agrupo y sumo las 4 columnas numéricas
            num_cols = ["cantidad_pedido","cantidad_entrega","iva_valor","impuesto_ultraprocesado"]
            for c in num_cols:
                df[c] = df[c].replace("","0").astype(float)
            # agrupación
            agrup = [
                col for col in [
                    "orden_de_compra","unidad_medida","codigo_sap_cliente",
                    "factura_sap","fecha_factura","material",
                    "descripcion_material","transporte","valor_unitario"
                ] if col in df.columns
            ]
            res = (
                df.groupby(agrup, as_index=False)
                  .agg({c:"sum" for c in num_cols})
            )
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                res.to_excel(w, sheet_name="Consolidado", index=False)
            buf.seek(0)
            return send_file(
                buf,
                as_attachment=True,
                download_name="consolidado_celluweb.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        else:
            # ---- ECOM: agrupamiento según tu SQL
            # defino campos de group-by exactamente
            group_by = [
                "pedido","orden_de_compra","cantidad_pedido","unidad_medida",
                "valor_pedido","vendedor","codigo_sap_cliente","nombre_cliente",
                "factura_sap","fecha_factura","ciudad","nit_compania",
                "codigo_barras_material","categoria","material",
                "descripcion_material","causa_no_despacho",
                "unidad_medida_facturada","iva","iva_valor","tipo_pos"
            ]
            # asegurar que existen en df
            group_by = [c for c in group_by if c in df.columns]
            # convertir tipos
            df["cantidad_facturada"] = df["cantidad_facturada"].replace("","0").astype(float)
            df["valor_unitario"]     = df["valor_unitario"].replace("","0").astype(float)
            df["valor_neto"]         = df["valor_neto"].replace("","0").astype(float)

            # construyo el DataFrame agrupado
            ecom = (
                df.groupby(group_by, as_index=False)
                  .agg({
                    "cantidad_facturada":"sum",
                    "valor_unitario":"mean",
                    "valor_neto":"sum"
                  })
            )
            # renombro la media de valor_unitario
            ecom = ecom.rename(columns={"valor_unitario":"PromedioDeValor_unitario"})
            # renombrar tipo_pos → Tipo de pedido
            if "tipo_pos" in ecom.columns:
                ecom = ecom.rename(columns={"tipo_pos":"Tipo de pedido"})

            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                ecom.to_excel(w, sheet_name="Ecom", index=False)
            buf.seek(0)
            return send_file(
                buf,
                as_attachment=True,
                download_name="consolidado_ecom.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    # GET → formulario
    return render_template("consolidar_compras.html")
