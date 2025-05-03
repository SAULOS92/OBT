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

# Columnas de entrada (exactas) para validación
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

# Campos numéricos a convertir antes de agrupar
NUMERIC_COLS = [
    "Cantidad del pedido",
    "Cantidad Entrega",
    "Iva_valor",
    "Impuesto Ultraprocesado",
    "Valor_unitario"
]

# Campos comunes para agrupar en ambos formatos
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

        # 1) Leer sin modificar nombres
        try:
            df = pd.read_excel(f, engine="openpyxl", dtype=str).fillna("")
        except Exception as e:
            flash(f"Error leyendo el Excel: {e}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # 2) Validar encabezados
        falt = [c for c in INPUT_COLS if c not in df.columns]
        if falt:
            flash(f"Faltan columnas en el Excel de entrada: {falt}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))
        dup = df.columns[df.columns.duplicated()].unique().tolist()
        if dup:
            flash(f"Encabezados duplicados en el Excel de entrada: {dup}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

        # 3) Prefiltrado ECOM: excluir ZCMM y ZCM2
        if fmt == "ecom":
            df = df[~df["Tipo Pos"].isin(["ZCMM", "ZCM2"])]

        # 4) Convertir a numérico
        for c in NUMERIC_COLS:
            df[c] = df[c].replace("", "0").astype(float)

        # 5) Agrupar segun formato
        try:
            if fmt == "ecom":
                # Conservamos Pedido, Orden de compra y Fecha Factura
                keys = GROUP_FIELDS + ["Pedido", "Orden de compra", "Fecha Factura"]
                agg = df.groupby(keys, as_index=False).agg({
                    "Cantidad del pedido": "sum",
                    "Cantidad Entrega":    "sum",
                    "Iva_valor":           "sum",
                    "Valor_unitario":      "mean"
                })
            else:  # celluweb
                agg = df.groupby(GROUP_FIELDS, as_index=False).agg({
                    "Cantidad del pedido":        "sum",
                    "Cantidad Entrega":            "sum",
                    "Iva_valor":                   "sum",
                    "Impuesto Ultraprocesado":     "sum",
                    "Valor_unitario":              "mean"
                })

            hoy = datetime.now().strftime("%Y%m%d")

            if fmt == "celluweb":
                # 6.1) Campos estáticos celluweb
                static_vals = {
                    "Pedido":         "0",
                    "Orden de compra":"23",
                    "Factura Sap":    "FC",
                    "Fecha Factura":  "0",
                    "Entrega":        "0",
                    "Pos.Entrega":    "0",
                    "Transporte":     "0"
                }
                for col, val in static_vals.items():
                    agg[col] = val

                df_out   = agg[INPUT_COLS]
                filename = f"consolidado_celuweb_{hoy}.xlsx"

            else:
                # 6.2) Campos estáticos ECOM (incluye Factura Sap)
                static_cols = {
                    "Factura Sap":          "FC",
                    "Valor_pedido":          "0",
                    "Vendedor":              "0",
                    "Nombre_cliente":        "0",
                    "Ciudad":                "0",
                    "Nit_compania":          "0",
                    "Codigo_barras_material":"0",
                    "Categoria":             "0",
                    "Causa_no_despacho":     "0",
                    "Iva":                   "0"
                }
                for col, val in static_cols.items():
                    agg[col] = val

                # 6.3) Cálculos adicionales
                agg["Valor_neto"]                 = agg["Valor_unitario"] * agg["Cantidad Entrega"]
                agg["Unidad_de_medida_facturada"] = agg["Unidad de medida"]

                # 6.4) Renombrar a snake_case
                rename_map = {
                    "Pedido":                 "Pedido",
                    "Orden de compra":        "Orden_de_compra",
                    "Fecha Factura":          "Fecha_factura",
                    "Cantidad del pedido":    "Cantidad_del_pedido",
                    "Unidad de medida":       "Unidad_de_medida",
                    "Codigo Sap Cliente":     "Codigo_sap_cliente",
                    "Factura Sap":            "Factura_sap",
                    "Material":               "Material",
                    "Descripcion del Material":"Descripcion_del_Material",
                    "Cantidad Entrega":       "Cantidad_facturada",
                    "Iva_valor":              "Iva_valor",
                    "Valor_unitario":         "Valor_unitario",
                    "Tipo Pos":               "Tipo_de_pedido"
                }
                agg = agg.rename(columns=rename_map)

                # 6.5) Orden exacto de columnas ECOM
                cols_ecom = [
                    "Pedido",
                    "Orden_de_compra",
                    "Cantidad_del_pedido",
                    "Unidad_de_medida",
                    "Valor_pedido",
                    "Vendedor",
                    "Codigo_sap_cliente",
                    "Nombre_cliente",
                    "Factura_sap",
                    "Fecha_factura",
                    "Ciudad",
                    "Nit_compania",
                    "Codigo_barras_material",
                    "Categoria",
                    "Material",
                    "Descripcion_del_Material",
                    "Causa_no_despacho",
                    "Cantidad_facturada",
                    "Unidad_de_medida_facturada",
                    "Iva",
                    "Iva_valor",
                    "Valor_unitario",
                    "Valor_neto",
                    "Tipo_de_pedido"
                ]
                df_out   = agg[cols_ecom]
                filename = f"consolidado_ecom_{hoy}.xlsx"

            # 7) Generar y enviar Excel
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

        except KeyError as e:
            flash(f"Error generando el informe ({fmt}): falta la columna {e}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))
        except Exception as e:
            flash(f"Error inesperado generando el informe ({fmt}): {e}", "error")
            return redirect(url_for("consolidar_compras.consolidar_compras_index"))

    # GET
    return render_template("consolidar_compras.html")






