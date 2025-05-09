import json
from io import BytesIO
import zipfile
from datetime import datetime
from views.auth import login_required


import pandas as pd
from flask import (
    Blueprint, render_template, request,
    flash, redirect, url_for, send_file, session
)
from db import conectar

generar_pedidos_bp = Blueprint(
    "generar_pedidos", __name__,
    template_folder="../templates"
)

# 1) Constantes de validación
GEN_HEADERS = {
    "materiales": ["pro_codigo", "particion", "pq_x_caja"],
    "inventario": ["codigo", "stock"]
}
GEN_COL_MAP = {
    "materiales": {
        "pro_codigo": ["Codigo SAP", "Código"],
        "particion":  ["Particion", "Partición"],
        "pq_x_caja":  ["Unidades x caja", "Unidad por Caja"]
    },
    "inventario": {
        "codigo":   ["Codigo articulo", "Cod Producto"],        
        "stock":    ["Unidades", "Unidades Disponibles"]
    }
}

def normalize_cols(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    inv_map = {}
    for internal, syns in col_map.items():
        for s in syns:
            inv_map[s.strip().lower()] = internal
    to_rename = {
        col: inv_map[col.strip().lower()]
        for col in df.columns
        if col.strip().lower() in inv_map
    }
    return df.rename(columns=to_rename)

@generar_pedidos_bp.route("/generar-pedidos", methods=["GET", "POST"])
@login_required 
def generar_pedidos_index():
    empresa = session.get('empresa')
    # ¿Mostramos botón de descarga tras POST exitoso?
    descarga = request.args.get("descarga", default=0, type=int)
    mostrar_descarga = bool(descarga)

    if request.method == "POST":
        try:
            # 2) Recoger y validar archivos
            f_mat = request.files.get("materiales")
            f_inv = request.files.get("inventario")
            if not f_mat or not f_inv:
                flash("Debes subir ambos archivos: Materiales e Inventario.", "error")
                return redirect(url_for("generar_pedidos.generar_pedidos_index"))

            # 3) Leer todo como texto y rellenar ""
            df_mat = pd.read_excel(f_mat, engine="openpyxl", dtype=str).fillna("")
            df_inv = pd.read_excel(f_inv, engine="openpyxl", dtype=str).fillna("")

            # 4) Normalizar columnas
            df_mat = normalize_cols(df_mat, GEN_COL_MAP["materiales"])
            df_inv = normalize_cols(df_inv, GEN_COL_MAP["inventario"])

            # 5) Validar encabezados faltantes
            falt = [h for h in GEN_HEADERS["materiales"] if h not in df_mat.columns]
            if falt:
                flash(f"Faltan columnas en Materiales: {falt}", "error")
                return redirect(url_for("generar_pedidos.generar_pedidos_index"))
            falt = [h for h in GEN_HEADERS["inventario"] if h not in df_inv.columns]
            if falt:
                flash(f"Faltan columnas en Inventario: {falt}", "error")
                return redirect(url_for("generar_pedidos.generar_pedidos_index"))

            # 6) Detectar duplicados
            dup = df_mat.columns[df_mat.columns.duplicated()].unique().tolist()
            if dup:
                flash(f"Columnas duplicadas en Materiales: {dup}", "error")
                return redirect(url_for("generar_pedidos.generar_pedidos_index"))
            dup = df_inv.columns[df_inv.columns.duplicated()].unique().tolist()
            if dup:
                flash(f"Columnas duplicadas en Inventario: {dup}", "error")
                return redirect(url_for("generar_pedidos.generar_pedidos_index"))

            # 7) Forzar numeric + 0
            df_mat["particion"] = df_mat["particion"].replace("", "0").astype(int)
            df_mat["pq_x_caja"] = df_mat["pq_x_caja"].replace("", "0").astype(int)
            df_inv["stock"]     = df_inv["stock"].replace("", "0").astype(int)

            # 8) Serializar y llamar al SP
            mat_json = df_mat[GEN_HEADERS["materiales"]].to_dict(orient="records")
            inv_json = df_inv[GEN_HEADERS["inventario"]].to_dict(orient="records")
            conn = conectar(); cur = conn.cursor()
            cur.execute(
"CALL sp_cargar_materiales(%s, %s);",
    (json.dumps(mat_json), empresa)
)

# Luego llamar al procedimiento que genera pedidos

            conn.commit()
            cur.close(); conn.close()
            conn = conectar(); cur = conn.cursor()
            cur.execute(
    "CALL sp_etl_pedxrutaxprod_json(%s, %s);",
    (json.dumps(inv_json), empresa)
)
            conn.commit()
            cur.close(); conn.close()

            # 9) Validar materiales sin definir
            conn = conectar(); cur = conn.cursor()
            cur.execute(
    "SELECT fn_materiales_sin_definir(%s);",
    (empresa,)
)
            raw_mis = cur.fetchone()[0]
            cur.close(); conn.close()

            mis = json.loads(raw_mis) if isinstance(raw_mis, str) else (raw_mis or [])
            if mis:
                detalles = ", ".join(f"{m['codigo_pro']}:{m['producto']}" for m in mis)
                flash(f"Materiales sin definir: {detalles}", "error")
                return redirect(url_for("generar_pedidos.generar_pedidos_index"))

            flash("Pedidos generados con éxito.", "success")
            # redirect con descarga=1 para mostrar botón
            return redirect(url_for("generar_pedidos.generar_pedidos_index", descarga=1))

        except Exception as e:
            flash(f"Error al generar pedidos: {e}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

    # GET → renderizar formulario + flag descarga
    return render_template(
        "generar_pedidos.html",
        mostrar_descarga=mostrar_descarga
    )

@generar_pedidos_bp.route("/generar-pedidos/descargar", methods=["GET"])
@login_required
def descargar_reportes():
    empresa = session.get('empresa')
    # 1) Traer los JSON desde la BD
    conn = conectar(); cur = conn.cursor()
    cur.execute(
    "SELECT fn_obtener_reparticion_inventario_json(%s);",
    (empresa,))
    raw_rep = cur.fetchone()[0]
    cur.execute(
    "SELECT fn_obtener_pedidos_con_pedir_json(%s);",
    (empresa,))
    raw_ped = cur.fetchone()[0]
    cur.close(); conn.close()

    data_rep = json.loads(raw_rep) if isinstance(raw_rep, str) else (raw_rep or [])
    data_ped = json.loads(raw_ped) if isinstance(raw_ped, str) else (raw_ped or [])

    # 2) Crear ZIP en memoria
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:

        # 2a) Hoja de repartición (única)
        b_rep = BytesIO()
        pd.DataFrame(data_rep)\
          .to_excel(b_rep, sheet_name="Reparticion", index=False, engine="openpyxl")
        b_rep.seek(0)
        zf.writestr("reparticion_inventario.xlsx", b_rep.read())

        # 2b) Un archivo por cada ruta distinta en data_ped
        rutas = sorted({ item["ruta"] for item in data_ped })
        for ruta in rutas:
            subset = [ d for d in data_ped if d["ruta"] == ruta ]
            df = pd.DataFrame(subset, columns=["codigo_pro","producto","pedir"])
            # Agregar la columna estática "UN"
            df.insert(2, "UN", "UN")

            buf = BytesIO()
            # startrow=3 coloca los datos a partir de la fila 4
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df.to_excel(
                    writer,
                    sheet_name=f"Ruta_{ruta}",
                    index=False,
                    startrow=3
                )
            buf.seek(0)
            zf.writestr(f"pedidos_ruta_{ruta}.xlsx", buf.read())

    zip_buf.seek(0)
    hoy = datetime.now().strftime("%Y%m%d")
    nombre_zip = f"reportes_{hoy}.zip"
    return send_file(
        zip_buf,
        as_attachment=True,
        download_name=nombre_zip,
        mimetype="application/zip"
    )






