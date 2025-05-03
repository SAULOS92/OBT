import json
import numpy as np
import pandas as pd
from io import BytesIO
from flask import (
    Blueprint, render_template, request,
    flash, redirect, url_for, send_file, session
)
from db import conectar
from views.auth import login_required

upload_bp = Blueprint("upload", __name__, template_folder="../templates")

# Tus constantes tal cual
PED_HEADERS = [
    "numero_pedido","hora","cliente","nombre","barrio","ciudad",
    "asesor","codigo_pro","producto","cantidad","valor",
    "tipo_pro","estado"
]
RUT_HEADERS   = ["codigo_cliente","codigo_ruta"]
DIAS_VALIDOS  = {"LU","MA","MI","JU","VI","SA","DO"}

PED_COL_MAP = {
    "numero_pedido": ["Pedido"],
    "hora":          ["Hora"],
    "cliente":       ["Cliente"],
    "nombre":        ["R. Social"],
    "barrio":        ["Barrio"],
    "ciudad":        ["Ciudad"],
    "asesor":        ["Asesor"],
    "codigo_pro":    ["Cod.Prod"],
    "producto":      ["Producto"],
    "cantidad":      ["Cantidad"],
    "valor":         ["Total"],
    "tipo_pro":      ["Tip Pro"],
    "estado":        ["Estado"]
}
RUT_COL_MAP = {
    "codigo_cliente": ["Cod. Cliente", "Código CW"],
    "codigo_ruta":    ["Ruta", "Descripción Ruta"]
}

def normalize_cols(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    inv = {}
    for internal, syns in col_map.items():
        for s in syns:
            inv[s.strip().lower()] = internal
    to_rename = {
        col: inv[col.strip().lower()]
        for col in df.columns
        if col.strip().lower() in inv
    }
    return df.rename(columns=to_rename)

@upload_bp.route("/", methods=["GET","POST"])
@upload_bp.route("/cargar-pedidos", methods=["GET","POST"])
@login_required 
def upload_index():
    descargar_flag = request.args.get("descarga", default=0, type=int)
    mostrar_descarga = bool(descargar_flag)

    if request.method == "POST":
        try:
            # 1) Archivos + día
            f_ped = request.files.get("pedidos")
            f_rut = request.files.get("rutas")
            p_dia = request.form.get("dia","").strip()
            if not f_ped or not f_rut or p_dia not in DIAS_VALIDOS:
                flash("Sube ambos archivos y selecciona un día válido.","error")
                return redirect(request.url)

            # 2) Leer todo como texto y fillna("")
            df_ped = pd.read_excel(f_ped, engine="openpyxl", dtype=str).fillna("")
            df_rut = pd.read_excel(f_rut, engine="openpyxl", dtype=str).fillna("")

            # 3) Normalizar nombres según COL_MAP
            df_ped = normalize_cols(df_ped, PED_COL_MAP)
            df_rut = normalize_cols(df_rut, RUT_COL_MAP)

            # 4) Validar encabezados faltantes
            falt_ped = [h for h in PED_HEADERS if h not in df_ped.columns]
            falt_rut = [h for h in RUT_HEADERS if h not in df_rut.columns]
            if falt_ped:
                flash(f"Faltan columnas en Pedidos: {falt_ped}","error")
                return redirect(request.url)
            if falt_rut:
                flash(f"Faltan columnas en Rutas: {falt_rut}","error")
                return redirect(request.url)

            # 5) Detectar duplicados
            dup_p = df_ped.columns[df_ped.columns.duplicated()].unique().tolist()
            dup_r = df_rut.columns[df_rut.columns.duplicated()].unique().tolist()
            if dup_p:
                flash(f"Encabezados duplicados en Pedidos: {dup_p}","error")
                return redirect(request.url)
            if dup_r:
                flash(f"Encabezados duplicados en Rutas: {dup_r}","error")
                return redirect(request.url)

            # 6) Rellenar nombre/barrio/ciudad faltantes
            for col in ("nombre","barrio","ciudad"):
                df_ped[col] = (
                    df_ped.groupby("cliente")[col]
                          .transform(lambda g: g.replace("", np.nan).ffill().bfill())
                          .fillna("")
                )

            # 7) Forzar numéricos y rellenar vacíos con cero
            df_ped["cantidad"] = df_ped["cantidad"].replace("","0").astype(int)
            df_ped["valor"]    = df_ped["valor"].replace("","0").astype(float)
            # codigo_pro queda como texto

            # 8) Serializar JSON y ejecutar SP
            pedidos = df_ped[PED_HEADERS].to_dict(orient="records")
            rutas   = df_rut[RUT_HEADERS].to_dict(orient="records")
            conn = conectar(); cur = conn.cursor()
            cur.execute(
                "CALL etl_cargar_pedidos_y_rutas_masivo(%s,%s,%s);",
                (json.dumps(pedidos), json.dumps(rutas), p_dia)
            )
            conn.commit(); cur.close(); conn.close()

            flash("¡Carga masiva exitosa!","success")
            # recarga con flag descarga=1
            return redirect(url_for("upload.upload_index", descarga=1))

        except Exception as e:
            flash(f"Error inesperado: {e}","error")
            return redirect(url_for("upload.upload_index"))

    return render_template(
        "upload.html",
        mostrar_descarga=mostrar_descarga
    )

@upload_bp.route("/cargar-pedidos/descargar-resumen", methods=["GET"])
@login_required 
def descargar_resumen():
    empresa = session.get('empresa')
    conn = conectar(); cur = conn.cursor()
    cur.execute("SELECT fn_obtener_resumen_pedidos();")
    raw = cur.fetchone()[0]
    cur.close(); conn.close()
    data = json.loads(raw) if isinstance(raw,str) else (raw or [])

    cols = ["codigo_cli","nombre","barrio","ciudad","asesor","total_pedidos","ruta"]
    df_res = pd.DataFrame(data, columns=cols)

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_res.to_excel(writer, sheet_name="ResumenPedidos", index=False)
    buf.seek(0)

    return send_file(
        buf,
        as_attachment=True,
        download_name="resumen_pedidos.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument-spreadsheetml.sheet"
    )


