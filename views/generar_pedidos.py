import json
from io import BytesIO
import zipfile

import pandas as pd
from flask import (
    Blueprint, render_template, request,
    flash, redirect, url_for, send_file
)
from db import conectar

generar_pedidos_bp = Blueprint(
    "generar_pedidos", __name__,
    template_folder="../templates"
)

# 1) Constantes de validación (idénticas)
GEN_HEADERS = {
    "materiales": ["pro_codigo", "particion", "pq_x_caja"],
    "inventario": ["codigo", "producto", "stock"]
}
GEN_COL_MAP = {
    "materiales": {
        "pro_codigo": ["pro_codigo", "Codigo SAP"],
        "particion":  ["particion",  "Particion"],
        "pq_x_caja":  ["pq_x_caja",  "Unidades x caja"]
    },
    "inventario": {
        "codigo":   ["codigo", "Codigo articulo"],
        "producto": ["producto", "Nombre articulo"],
        "stock":    ["stock", "Unidades"]
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
def generar_pedidos_index():
    # 1) ¿Mostramos botón de descarga?
    descarga = request.args.get("descarga", default=0, type=int)
    mostrar_descarga = bool(descarga)

    if request.method == "POST":
        try:
            # ——————————————————————————————————————
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
                "CALL sp_etl_pedxrutaxprod_json(%s, %s);",
                (json.dumps(mat_json), json.dumps(inv_json))
            )
            conn.commit()
            cur.close(); conn.close()

            flash("Pedidos generados con éxito.", "success")
            # — redirect indicando que luego mostramos descarga
            return redirect(url_for("generar_pedidos.generar_pedidos_index", descarga=1))

        except Exception as e:
            flash(f"Error al generar pedidos: {e}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

    # GET → render con flag de descarga
    return render_template(
        "generar_pedidos.html",
        mostrar_descarga=mostrar_descarga
    )


@generar_pedidos_bp.route("/generar-pedidos/descargar", methods=["GET"])
def descargar_reportes():
    # 9) Llamar a los informes y crear ZIP
    conn = conectar(); cur = conn.cursor()
    cur.execute("SELECT fn_obtener_reparticion_inventario_json();")
    raw_rep = cur.fetchone()[0]
    cur.execute("SELECT fn_obtener_pedidos_con_pedir_json();")
    raw_ped = cur.fetchone()[0]
    cur.close(); conn.close()

    data_rep = json.loads(raw_rep) if isinstance(raw_rep, str) else (raw_rep or [])
    data_ped = json.loads(raw_ped) if isinstance(raw_ped, str) else (raw_ped or [])

    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        # Hoja 1
        df1 = pd.DataFrame(data_rep)
        b1 = BytesIO()
        df1.to_excel(b1, sheet_name="Reparticion", index=False, engine="openpyxl")
        b1.seek(0)
        zf.writestr("reparticion_inventario.xlsx", b1.read())
        # Hoja 2
        df2 = pd.DataFrame(data_ped)
        b2 = BytesIO()
        df2.to_excel(b2, sheet_name="PedidosPorPedir", index=False, engine="openpyxl")
        b2.seek(0)
        zf.writestr("pedidos_por_pedir.xlsx", b2.read())

    zip_buf.seek(0)
    return send_file(
        zip_buf,
        as_attachment=True,
        download_name="reportes_generados.zip",
        mimetype="application/zip"
    )




