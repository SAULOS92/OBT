# views/upload.py

import json
import pandas as pd
from flask import Blueprint, render_template, request, flash, redirect, url_for
from db import conectar

upload_bp = Blueprint("upload", __name__, template_folder="../templates")

# Columnas internas esperadas por el SP
PED_HEADERS = [
    "numero_pedido","hora","cliente","nombre","barrio","ciudad",
    "asesor","codigo_pro","producto","cantidad","valor",
    "tipo_pro","estado"
]
RUT_HEADERS = ["codigo_cliente","codigo_ruta"]
DIAS_VALIDOS = {"LU","MA","MI","JU","VI","SA","DO"}

COL_MAP = {
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
    "estado":        ["estado","Estado"],
    "codigo_cliente":["Cod. Cliente"],
    "codigo_ruta":   ["Ruta"]
}

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    # 1) limpieza de nombres
    cleaned = {
        col: col.strip().lower().replace(" ", "_").replace("-", "_")
        for col in df.columns
    }
    df = df.rename(columns=cleaned)
    # 2) construye mapa inverso
    inv = {}
    for internal, syns in COL_MAP.items():
        for s in syns:
            key = s.strip().lower().replace(" ", "_").replace("-", "_")
            inv[key] = internal
    # 3) renombra
    return df.rename(columns={c: inv[c] for c in df.columns if c in inv})

@upload_bp.route("/", methods=["GET","POST"])
@upload_bp.route("/cargar-pedidos", methods=["GET","POST"])
def upload_index():
    if request.method == "POST":
        # archivos y día
        f_ped = request.files.get("pedidos")
        f_rut = request.files.get("rutas")
        p_dia = request.form.get("dia","").strip()

        if p_dia not in DIAS_VALIDOS:
            flash(f"Día inválido. Elige uno de: {', '.join(DIAS_VALIDOS)}","error")
            return redirect(url_for("upload.upload_index"))

        # leer Excel
        try:
            df_ped = pd.read_excel(f_ped, engine="openpyxl")
            df_rut = pd.read_excel(f_rut, engine="openpyxl")
        except Exception as e:
            flash(f"Error leyendo Excel: {e}","error")
            return redirect(url_for("upload.upload_index"))

        # normalizar nombres
        df_ped = normalize_cols(df_ped)
        df_rut = normalize_cols(df_rut)

        # detectar duplicados
        dup_p = df_ped.columns[df_ped.columns.duplicated()].unique().tolist()
        if dup_p:
            flash(f"Duplicados en Pedidos: {dup_p}","error")
            return redirect(url_for("upload.upload_index"))
        dup_r = df_rut.columns[df_rut.columns.duplicated()].unique().tolist()
        if dup_r:
            flash(f"Duplicados en Rutas: {dup_r}","error")
            return redirect(url_for("upload.upload_index"))

        # ————— Aquí: eliminación de NaN y conversión de tipos —————
        # reemplaza nan/pd.NA por None
        df_ped = df_ped.where(pd.notnull(df_ped), None)
        df_rut = df_rut.where(pd.notnull(df_rut), None)

        # convierte a str los campos TEXT
        df_ped["numero_pedido"] = df_ped["numero_pedido"].apply(
            lambda x: str(x) if x is not None else None
        )
        df_ped["cliente"] = df_ped["cliente"].apply(
            lambda x: str(x) if x is not None else None
        )
        df_rut["codigo_cliente"] = df_rut["codigo_cliente"].apply(
            lambda x: str(x) if x is not None else None
        )

        # convierte a int las columnas numéricas
        for col in ("codigo_pro","cantidad","valor"):
            if col in df_ped.columns:
                df_ped[col] = df_ped[col].apply(
                    lambda x: int(x) if x is not None else None
                )
        # ————————————————————————————————————————————————————————

        # validar encabezados
        falt = [h for h in PED_HEADERS if h not in df_ped.columns]
        if falt:
            flash(f"Faltan columnas en Pedidos: {falt}","error")
            return redirect(url_for("upload.upload_index"))
        falt = [h for h in RUT_HEADERS if h not in df_rut.columns]
        if falt:
            flash(f"Faltan columnas en Rutas: {falt}","error")
            return redirect(url_for("upload.upload_index"))

        # serializar
        pedidos = df_ped[PED_HEADERS].to_dict(orient="records")
        rutas   = df_rut[RUT_HEADERS].to_dict(orient="records")

        try:
            conn = conectar()
            cur = conn.cursor()
            cur.execute(
                "CALL etl_cargar_pedidos_y_rutas_masivo(%s,%s,%s);",
                (json.dumps(pedidos),json.dumps(rutas),p_dia)
            )
            conn.commit()
            flash("¡Carga masiva exitosa!","success")
        except Exception as e:
            flash(f"Error en ETL: {e}","error")
        finally:
            cur.close()
            conn.close()

        return redirect(url_for("upload.upload_index"))

    return render_template("upload.html")
