# views/generar_pedidos.py

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

# 1) Constantes de validación
GEN_HEADERS = {
    "materiales": ["pro_codigo", "particion", "pq_x_caja"],
    "inventario": ["codigo", "producto", "stock"]
}

GEN_COL_MAP = {
    "materiales": {
        "pro_codigo": ["pro_codigo", "Codigo SAP"],
        "particion":  ["particion", "Particion"],
        "pq_x_caja":  ["pq_x_caja", "Unidades x caja"]
    },
    "inventario": {
        "codigo":   ["codigo", "Codigo articulo"],
        "producto": ["producto", "Nombre articulo"],
        "stock":    ["stock", "Unidades"]
    }
}


def normalize_cols(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    """
    Renombra columnas según col_map.
    No limpia nombres, solo mapea sinónimos (case-insensitive).
    """
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
    if request.method == "POST":
        # 2) Recoger archivos
        f_mat = request.files.get("materiales")
        f_inv = request.files.get("inventario")
        if not f_mat or not f_inv:
            flash("Debes subir ambos archivos: Materiales e Inventario.", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

        # 3) Leer Excel como texto
        try:
            df_mat = pd.read_excel(f_mat, engine="openpyxl", dtype=str).fillna("")
            df_inv = pd.read_excel(f_inv, engine="openpyxl", dtype=str).fillna("")
        except Exception as e:
            flash(f"Error leyendo los Excel: {e}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

        # 4) Normalizar columnas (COL_MAP)
        df_mat = normalize_cols(df_mat, GEN_COL_MAP["materiales"])
        df_inv = normalize_cols(df_inv, GEN_COL_MAP["inventario"])

        # 5) Validar encabezados faltantes
        falt_mat = [h for h in GEN_HEADERS["materiales"] if h not in df_mat.columns]
        if falt_mat:
            flash(f"Faltan columnas en Materiales: {falt_mat}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))
        falt_inv = [h for h in GEN_HEADERS["inventario"] if h not in df_inv.columns]
        if falt_inv:
            flash(f"Faltan columnas en Inventario: {falt_inv}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

        # 6) Detectar duplicados
        dup_mat = df_mat.columns[df_mat.columns.duplicated()].unique().tolist()
        if dup_mat:
            flash(f"Columnas duplicadas en Materiales: {dup_mat}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))
        dup_inv = df_inv.columns[df_inv.columns.duplicated()].unique().tolist()
        if dup_inv:
            flash(f"Columnas duplicadas en Inventario: {dup_inv}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

        # 7) Forzar tipos numéricos y rellenar vacíos en cero
        try:
            # Materiales
            df_mat["particion"] = df_mat["particion"].replace("", "0").astype(int)
            df_mat["pq_x_caja"] = df_mat["pq_x_caja"].replace("", "0").astype(int)
            # Inventario
            df_inv["stock"] = df_inv["stock"].replace("", "0").astype(int)
        except ValueError as e:
            flash(f"Error en formato numérico: {e}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

        # 8) Serializar a JSONB
        mat_json = df_mat[GEN_HEADERS["materiales"]].to_dict(orient="records")
        inv_json = df_inv[GEN_HEADERS["inventario"]].to_dict(orient="records")

        # 9) Llamar al SP
        try:
            conn = conectar(); cur = conn.cursor()
            cur.execute(
                "CALL sp_etl_pedxrutaxprod_json(%s, %s);",
                (json.dumps(mat_json), json.dumps(inv_json))
            )
            conn.commit()
        except Exception as e:
            flash(f"Error al ejecutar SP: {e}", "error")
            cur.close(); conn.close()
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))
        finally:
            cur.close(); conn.close()

        # 10) Validar materiales sin definir
        try:
            conn = conectar(); cur = conn.cursor()
            cur.execute("SELECT fn_materiales_sin_definir();")
            raw_mis = cur.fetchone()[0]
            cur.close(); conn.close()
        except Exception as e:
            flash(f"Error validando materiales: {e}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

        mis = json.loads(raw_mis) if isinstance(raw_mis, str) else (raw_mis or [])
        if mis:
            detalles = ", ".join(f"{m['codigo_pro']}:{m['producto']}" for m in mis)
            flash(f"Materiales sin definir: {detalles}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

        # 11) Obtener reportes por separado
        try:
            conn = conectar(); cur = conn.cursor()
            cur.execute("SELECT fn_obtener_reparticion_inventario_json();")
            raw_rep = cur.fetchone()[0]
            cur.execute("SELECT fn_obtener_pedidos_con_pedir_json();")
            raw_ped = cur.fetchone()[0]
            cur.close(); conn.close()
        except Exception as e:
            flash(f"Error al obtener informes: {e}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

        data_rep = json.loads(raw_rep) if isinstance(raw_rep, str) else (raw_rep or [])
        data_ped = json.loads(raw_ped) if isinstance(raw_ped, str) else (raw_ped or [])

        # 12) Generar dos archivos Excel dentro de un ZIP
        zip_buf = BytesIO()
        with zipfile.ZipFile(zip_buf, "w") as zf:
            # Repartición
            buf1 = BytesIO()
            pd.DataFrame(data_rep).to_excel(
                buf1, sheet_name="Reparticion", index=False, engine="openpyxl"
            )
            buf1.seek(0)
            zf.writestr("reparticion_inventario.xlsx", buf1.read())

            # Pedidos por pedir
            buf2 = BytesIO()
            pd.DataFrame(data_ped).to_excel(
                buf2, sheet_name="PedidosPorPedir", index=False, engine="openpyxl"
            )
            buf2.seek(0)
            zf.writestr("pedidos_por_pedir.xlsx", buf2.read())

        zip_buf.seek(0)
        return send_file(
            zip_buf,
            as_attachment=True,
            download_name="reportes_generados.zip",
            mimetype="application/zip"
        )

    # GET → render form
    return render_template("generar_pedidos.html")



