"""
Blueprint: generar_pedidos_bp
────────────────────────────
• GET  /generar-pedidos     → Renderiza la página (solo HTML)
• POST /cargar-pedidos      → Recibe JSON (inventario + opc. materiales),
                              ejecuta los SP y devuelve un ZIP.
                              Si algo falla → JSON {error: <trace completo>}
"""

import json, traceback, logging
from io import BytesIO
import zipfile
from datetime import datetime

import pandas as pd
from flask import (
    Blueprint, render_template, request, jsonify,
    send_file, session
)
from views.auth import login_required
from db import conectar

LOG = logging.getLogger(__name__)

# ╔══════════════════════════════════════════════════════════════════════╗
# ║  Configuración                                                       ║
# ╚══════════════════════════════════════════════════════════════════════╝
GEN_HEADERS = {
    "materiales": ["pro_codigo", "particion", "pq_x_caja"],
    "inventario": ["codigo", "stock"],
}

generar_pedidos_bp = Blueprint(
    "generar_pedidos", __name__, template_folder="../templates"
)

# ╔══════════════════════════════════════════════════════════════════════╗
# ║  1) Página principal (GET)                                           ║
# ╚══════════════════════════════════════════════════════════════════════╝
@generar_pedidos_bp.route("/generar-pedidos", methods=["GET"])
@login_required
def generar_pedidos_index():
    return render_template("generar_pedidos.html", negocio=session.get("negocio"))


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  2) Endpoint AJAX (POST)                                             ║
# ╚══════════════════════════════════════════════════════════════════════╝
@generar_pedidos_bp.route("/cargar-pedidos", methods=["POST"])
@login_required
def cargar_pedidos():
    """Recibe JSON desde el frontend, llama SP y devuelve ZIP o error"""
    try:
        negocio = session.get("negocio")
        empresa = session.get("empresa")

        # ── 2.1 Leer payload ──────────────────────────────────────────
        payload = request.get_json(silent=True) or {}
        data_inv = payload.get("inventario") or []
        data_mat = payload.get("materiales")

        if not data_inv:
            raise ValueError("Inventario vacío o ausente")
        if negocio != "nutresa" and data_mat is None:
            raise ValueError("Falta enviar el archivo de materiales")

        # ── 2.2 DataFrame inventario ─────────────────────────────────
        df_inv = pd.DataFrame(data_inv)[GEN_HEADERS["inventario"]].fillna("")
        df_inv["stock"] = df_inv["stock"].replace("", "0").astype(int)

        conn = conectar(); cur = conn.cursor()
        cur.execute(
            "CALL sp_etl_pedxrutaxprod_json(%s, %s);",
            (json.dumps(df_inv.to_dict("records")), empresa),
        )
        conn.commit()

        # ── 2.3 DataFrame materiales (opcional) ─────────────────────
        if data_mat is not None and negocio != "nutresa":
            df_mat = pd.DataFrame(data_mat)[GEN_HEADERS["materiales"]].fillna("")
            for col in ("particion", "pq_x_caja"):
                df_mat[col] = (
                    df_mat[col].astype(str).str.strip().replace("", "1")
                    .astype(float).astype(int)
                )
            cur.execute(
                "CALL sp_cargar_materiales(%s, %s);",
                (json.dumps(df_mat.to_dict("records")), empresa),
            )
            conn.commit()

            # Materiales sin definir
            cur.execute("SELECT fn_materiales_sin_definir(%s);", (empresa,))
            mis = json.loads(cur.fetchone()[0] or "[]")
            if mis:
                sin_def = ", ".join(f"{m['codigo_pro']}:{m['producto']}" for m in mis)
                raise ValueError(f"Materiales sin definir: {sin_def}")

        cur.close(); conn.close()

        # ── 2.4 Construir ZIP ───────────────────────────────────────
        zip_buf = _build_zip(empresa)
        nombre_zip = datetime.now().strftime("formatos_%Y%m%d_%H%M.zip")

        return send_file(
            zip_buf, as_attachment=True,
            download_name=nombre_zip, mimetype="application/zip"
        )

    # ══ Captura cualquier excepción y devuelve el traceback completo ══
    except Exception as e:
        tb = traceback.format_exc()
        LOG.error(tb)                         # guarda en log del servidor
        return jsonify(error=tb), 500         # el frontend lo mostrará con showMsg


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  3) Helper: compone el ZIP con repartición + pedidos por ruta        ║
# ╚══════════════════════════════════════════════════════════════════════╝
def _build_zip(empresa: int) -> BytesIO:
    conn = conectar(); cur = conn.cursor()

    # Repartición inventario
    cur.execute("SELECT fn_obtener_reparticion_inventario_json(%s);", (empresa,))
    data_rep = json.loads(cur.fetchone()[0] or "[]")

    # Pedidos por ruta
    cur.execute("SELECT fn_obtener_pedidos_con_pedir_json(%s);", (empresa,))
    data_ped = json.loads(cur.fetchone()[0] or "[]")

    cur.close(); conn.close()

    # Renombra 'pedidos' -> 'pedir' si llegara en plural (robustez)
    for lst in (data_rep, data_ped):
        for row in lst:
            if "pedidos" in row and "pedir" not in row:
                row["pedir"] = row.pop("pedidos")

    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        # 1) Repartición
        col_rep = ["ruta","codigo_pro","producto","cantidad","pedir","ped99","inv"]
        df_rep = pd.DataFrame(data_rep)[col_rep]
        buf = BytesIO()
        df_rep.to_excel(buf, sheet_name="Reparticion", index=False, engine="openpyxl")
        buf.seek(0); zf.writestr("reparticion_inventario.xlsx", buf.read())

        # 2) Un .xlsx por ruta
        for ruta in sorted({r["ruta"] for r in data_ped}):
            subset = [r for r in data_ped if r["ruta"] == ruta]
            df = pd.DataFrame(subset, columns=["codigo_pro","producto","pedir"])
            df.insert(2, "UN", "UN")

            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as xls:
                df.to_excel(xls, sheet_name=f"Ruta_{ruta}", index=False, startrow=3)
            buf.seek(0)
            zf.writestr(f"pedidos_ruta_{ruta}.xlsx", buf.read())

    zip_buf.seek(0)
    return zip_buf







