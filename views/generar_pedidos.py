import json
from io import BytesIO

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
        "pro_codigo": ["pro_codigo", "Pro Codigo"],
        "particion":  ["particion",  "Particion"],
        "pq_x_caja":  ["pq_x_caja",  "PQ X Caja"]
    },
    "inventario": {
        "codigo":    ["codigo",    "Codigo"],
        "producto":  ["producto",  "Producto"],
        "stock":     ["stock",     "Stock"]
    }
}

def normalize_cols(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    """
    1) Limpia nombres: minúsculas, '_' en lugar de espacios/guiones.
    2) Renombra según col_map inverso.
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


@generar_pedidos_bp.route("/generar-pedidos", methods=["GET", "POST"])
def generar_pedidos_index():
    if request.method == "POST":
        # 2) Recoger archivos
        f_mat = request.files.get("materiales")
        f_inv = request.files.get("inventario")

        if not f_mat or not f_inv:
            flash("Debes subir ambos archivos: Materiales e Inventario.", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

        # 3) Leer Excel
        try:
            df_mat = pd.read_excel(f_mat, engine="openpyxl")
            df_inv = pd.read_excel(f_inv, engine="openpyxl")
        except Exception as e:
            flash(f"Error leyendo los Excel: {e}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

        # 4) Normalizar columnas
        df_mat = normalize_cols(df_mat, GEN_COL_MAP["materiales"])
        df_inv = normalize_cols(df_inv, GEN_COL_MAP["inventario"])

        # 5) Detectar duplicados
        dup_mat = df_mat.columns[df_mat.columns.duplicated()].unique().tolist()
        if dup_mat:
            flash(f"Columnas duplicadas en Materiales: {dup_mat}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))
        dup_inv = df_inv.columns[df_inv.columns.duplicated()].unique().tolist()
        if dup_inv:
            flash(f"Columnas duplicadas en Inventario: {dup_inv}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

        # 6) Validar encabezados
        falt_mat = [h for h in GEN_HEADERS["materiales"] if h not in df_mat.columns]
        if falt_mat:
            flash(f"Faltan columnas en Materiales: {falt_mat}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))
        falt_inv = [h for h in GEN_HEADERS["inventario"] if h not in df_inv.columns]
        if falt_inv:
            flash(f"Faltan columnas en Inventario: {falt_inv}", "error")
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))

        # 7) Rellenar vacíos con cero y convertir tipos
        df_mat = df_mat.fillna(0)
        df_inv = df_inv.fillna(0)

        for col in ("pro_codigo", "particion", "pq_x_caja"):
            df_mat[col] = df_mat[col].astype(int)
        df_inv["codigo"] = df_inv["codigo"].astype(int)
        df_inv["stock"]  = df_inv["stock"].astype(int)
        # producto queda string

        # 8) Serializar a JSONB
        mat_json = df_mat[GEN_HEADERS["materiales"]].to_dict(orient="records")
        inv_json = df_inv[GEN_HEADERS["inventario"]].to_dict(orient="records")

        # 9) Llamar al procedimiento almacenado
        try:
            conn = conectar()
            cur  = conn.cursor()
            # procedimiento: sp_etl_pedxrutaxprod_json(p_materiales JSONB, p_inventario JSONB)
            cur.execute(
                "CALL sp_etl_pedxrutaxprod_json(%s, %s);",
                (json.dumps(mat_json), json.dumps(inv_json))
            )
            conn.commit()
        except Exception as e:
            flash(f"Error al ejecutar el SP: {e}", "error")
            cur.close()
            conn.close()
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))
        finally:
            cur.close()
            conn.close()

        # 10) Llamar a la función que devuelve el informe
        try:
            conn = conectar()
            cur  = conn.cursor()
            cur.execute("SELECT fn_obtener_reparticion_inventario_json();")
            raw = cur.fetchone()[0]
        except Exception as e:
            flash(f"Error al obtener el informe: {e}", "error")
            cur.close()
            conn.close()
            return redirect(url_for("generar_pedidos.generar_pedidos_index"))
        cur.close()
        conn.close()

        # 11) Parsear el JSONB
        if isinstance(raw, str):
            data = json.loads(raw)
        elif isinstance(raw, (list, dict)):
            data = raw
        else:
            data = []

        # 12) Generar DataFrame y Excel
        cols = ["ruta", "codigo_pro", "producto", "cantidad", "pedir", "ped8_pq", "inv"]
        df_out = pd.DataFrame(data, columns=cols)

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_out.to_excel(writer, sheet_name="Reparticion", index=False)
        output.seek(0)

        return send_file(
            output,
            as_attachment=True,
            download_name="reparticion_inventario.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # GET → render form
    return render_template("generar_pedidos.html")
