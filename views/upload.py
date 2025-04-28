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
RUT_HEADERS = ["cliente", "dia", "codigo_ruta"]
DIAS_VALIDOS = {"LU", "MA", "MI", "JU", "VI", "SA", "DO"}

# Mapeo de sinónimos: clave interna -> lista de posibles encabezados (incluye el actual)
COL_MAP = {
    # Pedidos
    "numero_pedido": ["numero_pedido", "numero pedido", "num_pedido", "pedido id", "pedido-numero"],
    "hora":          ["hora", "tiempo", "time"],
    "cliente":       ["cliente", "cliente id", "id_cliente"],
    "nombre":        ["nombre", "cliente nombre"],
    "barrio":        ["barrio"],
    "ciudad":        ["ciudad"],
    "asesor":        ["asesor", "vendedor"],
    "codigo_pro":    ["codigo_pro", "codigo producto", "cod_pro", "producto_id"],
    "producto":      ["producto", "desc_producto"],
    "cantidad":      ["cantidad", "qty"],
    "valor":         ["valor", "precio", "monto"],
    "tipo":          ["tipo", "tipo_pedido"],
    "estado":        ["estado", "status"],

    # Rutas
    "cliente":     ["cliente", "cliente id", "id_cliente"],
    "dia":         ["dia", "día", "dia_semana"],
    "codigo_ruta": ["codigo_ruta", "codigo ruta", "cod_ruta", "ruta_id"]
}

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Limpia nombres: lower, strip, reemplaza espacios/guiones por '_'.
    2. Renombra según COL_MAP inverso.
    """
    # Paso 1: limpieza básica
    cleaned = {
        col: col.strip().lower().replace(" ", "_").replace("-", "_")
        for col in df.columns
    }
    df = df.rename(columns=cleaned)

    # Construye mapa inverso: cada alias limpio -> clave interna
    inv_map = {}
    for internal, synonyms in COL_MAP.items():
        for s in synonyms:
            key = s.strip().lower().replace(" ", "_").replace("-", "_")
            inv_map[key] = internal

    # Paso 2: renombrar columnas encontradas en inv_map
    to_rename = {
        col: inv_map[col]
        for col in df.columns
        if col in inv_map
    }
    return df.rename(columns=to_rename)
@upload_bp.route("/", methods=["GET", "POST"])
@upload_bp.route("/cargar-pedidos", methods=["GET", "POST"])
def upload_index():
    if request.method == "POST":
        f_ped = request.files.get("pedidos")
        f_rut = request.files.get("rutas")
        p_dia = request.form.get("dia", "").strip()

        # 1) Validar día
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

        # 3) Normalizar nombres de columnas
        df_ped = normalize_cols(df_ped)
        df_rut = normalize_cols(df_rut)

        # 4) Validar que existan todas las columnas internas requeridas
        falt_ped = [h for h in PED_HEADERS if h not in df_ped.columns]
        if falt_ped:
            flash(f"Faltan columnas en Pedidos: {falt_ped}", "error")
            return redirect(url_for("upload.upload_index"))

        falt_rut = [h for h in RUT_HEADERS if h not in df_rut.columns]
        if falt_rut:
            flash(f"Faltan columnas en Rutas: {falt_rut}", "error")
            return redirect(url_for("upload.upload_index"))

        # 5) Convertir a JSON y llamar al procedimiento
        pedidos = df_ped[PED_HEADERS].to_dict(orient="records")
        rutas   = df_rut[RUT_HEADERS].to_dict(orient="records")

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
