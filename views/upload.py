# views/upload.py

import json
import numpy as np
import pandas as pd
from flask import Blueprint, render_template, request, flash, redirect, url_for
from db import conectar

upload_bp = Blueprint(
    "upload", __name__,
    template_folder="../templates"
)

# Columnas internas que espera el SP; sustituimos "tipo" por "tipo_pro"
PED_HEADERS = [
    "numero_pedido", "hora", "cliente", "nombre", "barrio", "ciudad",
    "asesor", "codigo_pro", "producto", "cantidad", "valor",
    "tipo_pro",    # antes "tipo"
    "estado"
]
# Ahora rutas solo trae código de cliente y texto de ruta ("123-LU")
RUT_HEADERS = ["codigo_cliente", "codigo_ruta"]

# Valores válidos para el día de la semana
DIAS_VALIDOS = {"LU", "MA", "MI", "JU", "VI", "SA", "DO"}

# Mapeo de sinónimos: clave interna → posibles encabezados en Excel
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
    "tipo_pro":      ["tipo_pro", "Tipo", "Tip Pro"],
    "estado":        ["estado", "Estado"],

    # Rutas
    "codigo_cliente": ["Cod. Cliente"],
    "codigo_ruta":    ["Ruta"]
}


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Limpia nombres: minúsculas, quita espacios/guiones → '_'
    2. Renombra columnas según COL_MAP inverso
    """
    # 1) limpieza básica
    cleaned = {
        col: col.strip().lower().replace(" ", "_").replace("-", "_")
        for col in df.columns
    }
    df = df.rename(columns=cleaned)

    # 2) invierte COL_MAP para alias→interno
    inv_map = {}
    for internal, synonyms in COL_MAP.items():
        for s in synonyms:
            key = s.strip().lower().replace(" ", "_").replace("-", "_")
            inv_map[key] = internal

    # 3) renombra las que matcheen
    to_rename = {col: inv_map[col] for col in df.columns if col in inv_map}
    return df.rename(columns=to_rename)


@upload_bp.route("/", methods=["GET", "POST"])
@upload_bp.route("/cargar-pedidos", methods=["GET", "POST"])
def upload_index():
    if request.method == "POST":
        # 1) archivos y día
        f_ped = request.files.get("pedidos")
        f_rut = request.files.get("rutas")
        p_dia = request.form.get("dia", "").strip()

        # 2) validar día
        if p_dia not in DIAS_VALIDOS:
            flash(f"Día inválido. Elige uno de: {', '.join(DIAS_VALIDOS)}", "error")
            return redirect(url_for("upload.upload_index"))

        # 3) leer Excel
        try:
            df_ped = pd.read_excel(f_ped, engine="openpyxl")
            df_rut = pd.read_excel(f_rut, engine="openpyxl")
        except Exception as e:
            flash(f"Error leyendo los Excel: {e}", "error")
            return redirect(url_for("upload.upload_index"))

        # 4) normalizar nombres
        df_ped = normalize_cols(df_ped)
        df_rut = normalize_cols(df_rut)

        # 5) detectar duplicados
        dupes_p = df_ped.columns[df_ped.columns.duplicated()].unique().tolist()
        if dupes_p:
            flash(f"Columnas duplicadas en Pedidos: {dupes_p}", "error")
            return redirect(url_for("upload.upload_index"))
        dupes_r = df_rut.columns[df_rut.columns.duplicated()].unique().tolist()
        if dupes_r:
            flash(f"Columnas duplicadas en Rutas: {dupes_r}", "error")
            return redirect(url_for("upload.upload_index"))

        # 6) convertir NaN → None
        df_ped = df_ped.replace({np.nan: None})
        df_rut = df_rut.replace({np.nan: None})

        # 7) validar encabezados pedidos
        falt_ped = [h for h in PED_HEADERS if h not in df_ped.columns]
        if falt_ped:
            flash(f"Faltan columnas en Pedidos: {falt_ped}", "error")
            return redirect(url_for("upload.upload_index"))

        # 8) validar encabezados rutas
        falt_rut = [h for h in RUT_HEADERS if h not in df_rut.columns]
        if falt_rut:
            flash(f"Faltan columnas en Rutas: {falt_rut}", "error")
            return redirect(url_for("upload.upload_index"))

        # 9) serializar sólo las columnas que usa el SP
        pedidos = df_ped[PED_HEADERS].to_dict(orient="records")
        rutas   = df_rut[RUT_HEADERS].to_dict(orient="records")

        # 10) llamar al SP
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

    # GET → mostrar formulario
    return render_template("upload.html")




