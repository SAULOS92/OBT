import os
import pandas as pd
from io import BytesIO
from datetime import datetime
from flask import (
    Blueprint, render_template, request,
    flash, redirect, url_for, send_file,
    current_app, session
)
from views.auth import login_required

consolidar_bp = Blueprint(
    "consolidar_compras", __name__,
    template_folder="../templates"
)

# 1) Configuración de columnas de entrada
#    action: group | sum | mean | static
#    static_value: solo si action=='static'
COLUMN_CONFIG = {
    "Pedido":                  {"action": "static", "static_value": "0"},
    "Orden de compra":         {"action": "static", "static_value": "23"},
    "Cantidad del pedido":     {"action": "sum"},
    "Unidad de medida":        {"action": "group"},
    "Codigo Sap Cliente":      {"action": "group"},
    "Factura Sap":             {"action": "static", "static_value": "0"},
    "Fecha Factura":           {"action": "static", "static_value": "0"},
    "Material":                {"action": "group"},
    "Descripcion del Material":{"action": "group"},
    "Cantidad Entrega":        {"action": "sum"},
    "Iva_valor":               {"action": "sum"},
    "Impuesto Ultraprocesado": {"action": "sum"},
    "Valor_unitario":          {"action": "mean"},
    "Tipo Pos":                {"action": "group"},
    "Entrega":                 {"action": "static", "static_value": "0"},
    "Pos.Entrega":             {"action": "static", "static_value": "0"},
    "Transporte":              {"action": "static", "static_value": "0"},
}

# 2) Configuración de columnas finales ECOM
#    type: agg | static | calc
#    source: columna en 'agg' (solo para agg)
#    static_value: valor fijo (solo para static)
#    sources + func: definición de cálculo (solo para calc)
ECOM_COLUMN_SPEC = {
    "Pedido":                  {"type": "static", "static_value": "0"},
    "Orden_de_compra":         {"type": "static", "static_value": "23"},
    "Cantidad_del_pedido":     {"type": "static", "static_value": "0"},
    "Unidad_de_medida":        {"type": "static", "static_value": "0"},
    "Valor_pedido":            {"type": "static", "static_value": "0"},
    "Vendedor":                {"type": "static", "static_value": "0"},
    "Codigo_sap_cliente":      {"type": "static", "static_value": "0"},
    "Nombre_cliente":          {"type": "static", "static_value": "0"},
    "Factura_sap":             {"type": "static", "static_value": "FC"},
    "Fecha_factura":           {"type": "static", "static_value": "0"},
    "Ciudad":                  {"type": "static", "static_value": "0"},
    "Nit_compania":            {"type": "static", "static_value": "0"},
    "Codigo_barras_material":  {"type": "static", "static_value": "0"},
    "Categoria":               {"type": "static", "static_value": "0"},
    "Material":                {"type": "agg",    "source": "Material"},
    "Descripcion_del_Material":{"type": "agg",    "source": "Descripcion del Material"},
    "Causa_no_despacho":       {"type": "static", "static_value": "0"},
    "Cantidad_facturada":      {"type": "agg",    "source": "Cantidad Entrega"},
    "Unidad_de_medida_facturada":      {"type": "static", "static_value": "0"},
    "Iva":                     {"type": "static", "static_value": "0"},
    "Iva_valor":               {"type": "static", "static_value": "0"},
    "Valor_unitario":          {"type": "agg",    "source": "Valor_unitario"},
    "Valor_neto":              {
                                  "type":    "calc",
                                  "sources": ["Valor_unitario", "Cantidad Entrega"],
                                  "func":    "mul"
                                },
    "Tipo_de_pedido":          {"type": "agg",    "source": "Tipo Pos"}
}
ECOM_COLUMNS = list(ECOM_COLUMN_SPEC.keys())


@consolidar_bp.route("/consolidar-compras", methods=["GET", "POST"])
@login_required
def consolidar_compras_index():
    empresa = session.get('empresa')
    download_filename = None

    if request.method == "POST":
        fmt = request.form.get("format")
        f   = request.files.get("archivo")
        if not f or fmt not in ("celluweb", "ecom"):
            flash("Sube un Excel y elige un formato válido.", "error")
            return redirect(url_for(".consolidar_compras_index"))

        try:
            # --- Lectura y validación de columnas de entrada ---
            df = pd.read_excel(f, engine="openpyxl", dtype=str).fillna("")
            falt = [c for c in COLUMN_CONFIG if c not in df.columns]
            if falt:
                flash(f"Faltan columnas: {falt}", "error")
                return redirect(url_for(".consolidar_compras_index"))

            # --- Conversión numérica para sum/mean ---
            for col, cfg in COLUMN_CONFIG.items():
                if cfg["action"] in ("sum", "mean"):
                    df[col] = df[col].replace("", "0").astype(float)

            # --- Prefiltrado ECOM (excluir ZCMM y ZCM2) ---
            if fmt == "ecom":
                df = df[~df["Tipo Pos"].isin(["ZCMM", "ZCM2"])]

            # --- Preparar agrupación ---
            group_cols = [c for c, cfg in COLUMN_CONFIG.items() if cfg["action"] == "group"]
            agg_map    = {c: cfg["action"] for c, cfg in COLUMN_CONFIG.items() if cfg["action"] in ("sum", "mean")}

            # --- Agrupar ---
            agg = df.groupby(group_cols, as_index=False).agg(agg_map)

            # --- Asignar estáticos generales (COLUMN_CONFIG) ---
            for col, cfg in COLUMN_CONFIG.items():
                if cfg["action"] == "static":
                    agg[col] = cfg["static_value"]

            # --- Generar df_out y filename ---
            hoy = datetime.now().strftime("%Y%m%d")
            if fmt == "celluweb":
                df_out   = agg[list(COLUMN_CONFIG.keys())]
                filename = f"consolidado_celuweb_{hoy}.xlsx"
            else:
                # cálculo Valor_neto
                s0, s1 = ECOM_COLUMN_SPEC["Valor_neto"]["sources"]
                agg["Valor_neto"] = agg[s0] * agg[s1]
                # construir df_out
                df_out = pd.DataFrame({
                    new_col: (
                        agg[spec["source"]]
                        if spec["type"] == "agg" else
                        (spec["static_value"]
                         if spec["type"] == "static" else
                         agg["Valor_neto"])
                    )
                    for new_col, spec in ECOM_COLUMN_SPEC.items()
                })[ECOM_COLUMNS]
                filename = f"consolidado_ecom_{hoy}.xlsx"

            # --- Guardar Excel en buffer ---
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df_out.to_excel(writer, index=False)
            buf.seek(0)

            tmp_dir = os.path.join(current_app.root_path, "tmp")
            os.makedirs(tmp_dir, exist_ok=True)

           # --- Guardar Excel en tmp ---
            path = os.path.join(tmp_dir, filename)
            with open(path, "wb") as out:
                out.write(buf.getvalue())

            if fmt == "ecom":
                # (1) Generar CSV adicional para e-com
                csv_df = pd.DataFrame({
                   "bodega":          ["01"] * len(df_out),
                   "codigo_producto": df_out["Material"],
                   "cantidad":        df_out["Cantidad_facturada"],
                   "costo":           0
                })
                csv_filename = f"cargue_sugerido_{hoy}.csv"
                csv_path     = os.path.join(tmp_dir, csv_filename)
                csv_df.to_csv(csv_path, index=False)

                # exponer ambos archivos
                download_filename = [filename, csv_filename]
            else:
                # solo celluweb
                download_filename = filename
            flash("Consolidado generado con éxito.", "success")

        except Exception as e:
            flash(f"Error procesando informe {fmt}: {e}", "error")

    return render_template(
        "consolidar_compras.html",
        download_filename=download_filename
    )


@consolidar_bp.route("/consolidar-compras/download/<filename>")
@login_required  
def descargar_archivo_file(filename):
    tmp_dir = os.path.join(current_app.root_path, "tmp")
    path    = os.path.join(tmp_dir, filename)
    if not os.path.exists(path):
        flash("El archivo ya no está disponible.", "error")
        return redirect(url_for(".consolidar_compras_index"))
    mimetype = (
        "text/csv"
        if filename.lower().endswith(".csv")
        else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    return send_file(
        path,
        as_attachment=True,
        download_name=filename,
        mimetype=mimetype
    )









