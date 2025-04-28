import json
import pandas as pd
from flask import (
    Blueprint, render_template, request,
    flash, redirect, url_for
)
from db import conectar

upload_bp = Blueprint(
    "upload", __name__,
    template_folder="../templates"
)

# Columnas internas que espera el SP
PED_HEADERS = [
    "numero_pedido", "hora", "cliente", "nombre", "barrio", "ciudad",
    "asesor", "codigo_pro", "producto", "cantidad", "valor", "tipo", "estado"
]
# Ahora sólo cliente + codigo_ruta (texto con "123-LU")
RUT_HEADERS = ["cliente", "codigo_ruta"]

DIAS_VALIDOS = {"LU", "MA", "MI", "JU", "VI", "SA", "DO"}

# Mapeo de sinónimos: clave interna -> lista de posibles encabezados
COL_MAP = {
    # Pedidos
    "numero_pedido": ["numero_pedido", "Pedido"],
    "hora":          ["hora", "Hora"],
    "cliente":       ["cliente", "Cliente"],
    "nombre":        ["nombre", "R. Social"],
    "barrio":        ["barrio", "Barrio"],
    "ciudad":        ["ciudad", "Ciudad"],
    "asesor":        ["asesor", "Asesor"],
    "codigo_pro":    ["codigo_pro", "Cod.Prod"],
    "producto":      ["producto", "Producto"],
    "cantidad":      ["cantidad", "Cantidad"],
    "valor":         ["valor", "Total"],
    "tipo":          ["tipo", "Tip Pro"],
    "estado":        ["estado", "Estado"],

    # Rutas
    "cliente":      ["cliente", "Cod. Cliente"],
    "codigo_ruta":  ["codigo_ruta", "Ruta", "ruta"]
}

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Limpia nombres: lower, strip, espacios/guiones → '_'.
    2. Renombra según COL_MAP inverso.
    """
    cleaned = {
        col: col.strip().lower().replace(" ", "_").replace("-", "_")
        for col in df.columns
    }
    df = df.rename(columns=cleaned)

    inv_map = {}
    for internal, syns in COL_MAP.items():
        for s in syns:
            key = s.strip().lower().replace(" ", "_").replace("-", "_")
            inv_map[key] = internal

    to_rename = {col: inv_map[col] for col in df.columns if col in inv_map}
    return df.rename(columns=to_rename)


@upload_bp.route("/", methods=["GET", "POST"])
@upload_bp.route("/cargar-pedidos", methods=["GET", "POST"])
def upload_index():
    if request.method == "POST":
        f_ped = request.files.get("pedidos")
        f_rut = request.files.get("rutas")
        p_dia = request.form.get("dia", "").strip()

        # 1) Validar día del formulario
        if p_dia not in DIAS_VALIDOS:
            flash(f"Día inválido. Elige uno de: {', '.join(DIAS_VALIDOS)}", "error")
            return redirect(url_for("upload.upload_index"))

        # 2) Leer los Excel
        try:
            df_ped = pd.read_excel(f_ped, engine="openpyxl")
            df_rut = pd.read_excel(f_rut, engine="openpyxl")
        except Exception as e:
            flash(f"Error leyendo los Excel: {e}", "error")
            return redirect(url_for("upload.upload_index"))

        # 3) Normalizar nombres
        df_ped = normalize_cols(df_ped)
        df_rut = normalize_cols(df_rut)

        
 # 3.1) Detectar duplicados de encabezados
        dupes_p = df_ped.columns[df_ped.columns.duplicated()].unique().tolist()
        if dupes_p:
            flash(f"Columnas duplicadas en Pedidos: {dupes_p}", "error")
            return redirect(url_for("upload.upload_index"))
        dupes_r = df_rut.columns[df_rut.columns.duplicated()].unique().tolist()
        if dupes_r:
            flash(f"Columnas duplicadas en Rutas: {dupes_r}", "error")
            return redirect(url_for("upload.upload_index"))

        # 3.2) Reemplazar NaN por None
        import numpy as np
        df_ped = df_ped.replace({np.nan: None})
        df_rut = df_rut.replace({np.nan: None})

        # 4) Validar columnas de Pedidos
        falt_ped = [h for h in PED_HEADERS if h not in df_ped.columns]
        if falt_ped:
            flash(f"Faltan columnas en Pedidos: {falt_ped}", "error")
            return redirect(url_for("upload.upload_index"))

        # 5) Validar columnas de Rutas (cliente + codigo_ruta)
        falt_rut = [h for h in RUT_HEADERS if h not in df_rut.columns]
        if falt_rut:
            flash(f"Faltan columnas en Rutas: {falt_rut}", "error")
            return redirect(url_for("upload.upload_index"))

        # 6) Serializar sólo las columnas que usa el SP
        pedidos = df_ped[PED_HEADERS].to_dict(orient="records")
        rutas   = df_rut[RUT_HEADERS].to_dict(orient="records")

        # 7) Llamar al SP
        try:
            conn = conectar()
            cur = conn.cursor()
            cur.execute(
                "CALL etl_cargar_pedidos_y_rutas_masivo(%s, %s, %s);",
                (json.dumps(pedidos), json.dumps(rutas), p_dia)
            )
            conn.commit()
            flash("¡Carga masiva exitosa!", "success")
        except Exception as e:
            flash(f"Error en ETL: {e}", "error")
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("upload.upload_index"))

    return render_template("upload.html")

