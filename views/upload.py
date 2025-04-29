# views/upload.py

import json
import pandas as pd
from flask import Blueprint, render_template, request, flash, redirect, url_for
from db import conectar

upload_bp = Blueprint("upload", __name__, template_folder="../templates")

# Columnas internas que espera el SP
PED_HEADERS = [
    "numero_pedido","hora","cliente","nombre","barrio","ciudad",
    "asesor","codigo_pro","producto","cantidad","valor",
    "tipo_pro","estado"
]
RUT_HEADERS = ["codigo_cliente","codigo_ruta"]
DIAS_VALIDOS = {"LU","MA","MI","JU","VI","SA","DO"}

# Mapas separados para Pedidos y Rutas
PED_COL_MAP = {
    "numero_pedido": ["numero_pedido","Pedido"],
    "hora":          ["hora","Hora"],
    "cliente":       ["cliente","Cliente"],
    "nombre":        ["nombre","R. Social"],
    "barrio":        ["barrio","Barrio"],
    "ciudad":        ["ciudad","Ciudad"],
    "asesor":        ["asesor","Asesor"],
    "codigo_pro":    ["codigo_pro","Cod.Prod"],
    "producto":      ["producto","Producto"],
    "cantidad":      ["cantidad","Cantidad"],
    "valor":         ["valor","Total"],
    "tipo_pro":      ["tipo_pro","Tip Pro"],
    "estado":        ["estado","Estado"]
}

RUT_COL_MAP = {
    "codigo_cliente": ["Cod. Cliente"],
    "codigo_ruta":    ["Ruta"]
}

def normalize_cols(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    """
    1) Limpia nombres (minusculas, '_' en lugar de espacios/guiones)
    2) Renombra según col_map inverso
    """
    cleaned = {
        col: col.strip().lower().replace(" ", "_").replace("-", "_")
        for col in df.columns
    }
    df = df.rename(columns=cleaned)

    inv = {}
    for internal, syns in col_map.items():
        for s in syns:
            key = s.strip().lower().replace(" ", "_").replace("-", "_")
            inv[key] = internal

    to_rename = {c: inv[c] for c in df.columns if c in inv}
    return df.rename(columns=to_rename)


@upload_bp.route("/", methods=["GET","POST"])
@upload_bp.route("/cargar-pedidos", methods=["GET","POST"])
def upload_index():
    if request.method == "POST":
        # 1) Recoger archivos y día
        f_ped = request.files.get("pedidos")
        f_rut = request.files.get("rutas")
        p_dia = request.form.get("dia","").strip()

        # 2) Validar día
        if p_dia not in DIAS_VALIDOS:
            flash(f"Día inválido. Elige uno de: {', '.join(DIAS_VALIDOS)}", "error")
            return redirect(url_for("upload.upload_index"))

        # 3) Leer Excel
        try:
            df_ped = pd.read_excel(f_ped, engine="openpyxl")
            df_rut = pd.read_excel(f_rut, engine="openpyxl")
        except Exception as e:
            flash(f"Error leyendo los Excel: {e}", "error")
            return redirect(url_for("upload.upload_index"))

        # 4) Normalizar columnas con sus respectivos mapas
        df_ped = normalize_cols(df_ped, PED_COL_MAP)
        df_rut = normalize_cols(df_rut, RUT_COL_MAP)

        # 5) Detectar duplicados
        dupes_p = df_ped.columns[df_ped.columns.duplicated()].unique().tolist()
        if dupes_p:
            flash(f"Columnas duplicadas en Pedidos: {dupes_p}", "error")
            return redirect(url_for("upload.upload_index"))
        dupes_r = df_rut.columns[df_rut.columns.duplicated()].unique().tolist()
        if dupes_r:
            flash(f"Columnas duplicadas en Rutas: {dupes_r}", "error")
            return redirect(url_for("upload.upload_index"))

        # 6) Validar encabezados antes de continuar
        falt_ped = [h for h in PED_HEADERS if h not in df_ped.columns]
        if falt_ped:
            flash(f"Faltan columnas en Pedidos: {falt_ped}", "error")
            return redirect(url_for("upload.upload_index"))
        falt_rut = [h for h in RUT_HEADERS if h not in df_rut.columns]
        if falt_rut:
            flash(f"Faltan columnas en Rutas: {falt_rut}", "error")
            return redirect(url_for("upload.upload_index"))

        # 7) Rellenar todos los vacíos con cero
        df_ped = df_ped.fillna(0)
        df_rut = df_rut.fillna(0)

        # 8) Forzar que ciertos campos sean numéricos
        df_ped["codigo_pro"] = df_ped["codigo_pro"].astype(int)
        df_ped["cantidad"]   = df_ped["cantidad"].astype(int)
        df_ped["valor"]      = df_ped["valor"].astype(float)

        # 9) Serializar JSON
        pedidos = df_ped[PED_HEADERS].to_dict(orient="records")
        rutas   = df_rut[RUT_HEADERS].to_dict(orient="records")

        # 10) Llamar al procedimiento
        try:
            conn = conectar(); cur = conn.cursor()
            cur.execute(
                "CALL etl_cargar_pedidos_y_rutas_masivo(%s, %s, %s);",
                (json.dumps(pedidos), json.dumps(rutas), p_dia)
            )
            conn.commit()
            flash("¡Carga masiva exitosa!", "success")
        except Exception as e:
            flash(f"Error en ETL: {e}", "error")
        finally:
            cur.close(); conn.close()

        return redirect(url_for("upload.upload_index"))

    return render_template("upload.html")

