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
# Rutas solo trae código de cliente y texto de ruta ("123-LU")
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
    "tipo_pro":      ["tipo_pro", "Tip Pro"],
    "estado":        ["estado", "Estado"],

    # Rutas
    "codigo_cliente": ["Cod. Cliente", "cliente", "Cliente", "cliente_id"],
    "codigo_ruta":    ["Ruta", "ruta", "codigo_ruta"]
}

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Limpia nombres: minúsculas, sin espacios ni guiones (→ '_').
    2. Renombra columnas según COL_MAP inverso.
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
        # 1) Recoger archivos y día
        f_ped = request.files.get("pedidos")
        f_rut = request.files.get("rutas")
        p_dia = request.form.get("dia", "").strip()

        # 2) Validar día
        if p_dia not in DIAS_VALIDOS:
            flash(f"Día inválido. Elige uno de: {', '.join(DIAS_VALIDOS)}", "error")
            return redirect(url_for("upload.upload_index"))

        # 3) Leer los Excel
        try:
            df_ped = pd.read_excel(f_ped, engine="openpyxl")
            df_rut = pd.read_excel(f_rut, engine="openpyxl")
        except Exception as e:
            flash(f"Error leyendo los Excel: {e}", "error")
            return redirect(url_for("upload.upload_index"))

        # 4) Normalizar nombres de columnas
        df_ped = normalize_cols(df_ped)
        df_rut = normalize_cols(df_rut)

        # 5) Detectar columnas duplicadas
        dupes_p = df_ped.columns[df_ped.columns.duplicated()].unique().tolist()
        if dupes_p:
            flash(f"Columnas duplicadas en Pedidos: {dupes_p}", "error")
            return redirect(url_for("upload.upload_index"))
        dupes_r = df_rut.columns[df_rut.columns.duplicated()].unique().tolist()
        if dupes_r:
            flash(f"Columnas duplicadas en Rutas: {dupes_r}", "error")
            return redirect(url_for("upload.upload_index"))

        # 6) Reemplazar NaN por None para serializar correctamente
        df_ped = df_ped.replace({np.nan: None})
        df_rut = df_rut.replace({np.nan: None})

        # 6.1) Convertir a int las columnas que el SP espera como INTEGER
        for col in ("codigo_pro", "cantidad"):
            if col in df_ped.columns:
                df_ped[col] = df_ped[col].map(
                    lambda v: int(v) if (v is not None and not isinstance(v, str)) else v
                )

        # 6.2) Asegurar que cliente y codigo_cliente sean strings sin “.0”
        if "cliente" in df_ped.columns:
            df_ped["cliente"] = df_ped["cliente"].map(
                lambda v: None if v is None else str(int(v)) if isinstance(v, float) else str(v)
            )
        if "codigo_cliente" in df_rut.columns:
            df_rut["codigo_cliente"] = df_rut["codigo_cliente"].map(
                lambda v: None if v is None else str(int(v)) if isinstance(v, float) else str(v)
            )

        # 7) Validar columnas de Pedidos
        falt_ped = [h for h in PED_HEADERS if h not in df_ped.columns]
        if falt_ped:
            flash(f"Faltan columnas en Pedidos: {falt_ped}", "error")
            return redirect(url_for("upload.upload_index"))

        # 8) Validar columnas de Rutas
        falt_rut = [h for h in RUT_HEADERS if h not in df_rut.columns]
        if falt_rut:
            flash(f"Faltan columnas en Rutas: {falt_rut}", "error")
            return redirect(url_for("upload.upload_index"))

        # 9) Serializar solo las columnas que usa el SP
        pedidos = df_ped[PED_HEADERS].to_dict(orient="records")
        rutas   = df_rut[RUT_HEADERS].to_dict(orient="records")

        # 10) Llamar al procedimiento almacenado
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

    # GET → Mostrar el formulario
    return render_template("upload.html")





