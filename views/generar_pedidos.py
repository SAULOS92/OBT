"""
Blueprint: generar_pedidos_bp   •  Depurado + logging detallado
────────────────────────────────────────────────────────────────────
GET  /generar-pedidos      → página HTML
POST /cargar-pedidos       → recibe JSON, ejecuta SP, construye ZIP
                             • 400 → error controlado (ValueError)
                             • 500 → traceback completo
El objetivo es registrar en log **cada tramo crítico** y devolver al
frontend la información suficiente para diagnosticar fallos.
"""

import json, traceback, logging, uuid
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

# ─────────────────────────── Configuración logging ──────────────────────────
LOG = logging.getLogger(__name__)
if not LOG.handlers:                             # evita duplicar handlers
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
    ))
    LOG.addHandler(h)
    LOG.setLevel(logging.INFO)

# ─────────────────────────── Constantes encabezados ─────────────────────────
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
    """
    • Valida JSON recibido
    • Llama SP de inventario y (opc.) materiales
    • Verifica materiales indefinidos
    • Construye y devuelve ZIP
    En cada tramo se añade un log .info() y, si se lanza excepción
    controlada (ValueError), se devuelve como 400 con mensaje.
    """
    trace_id = uuid.uuid4().hex[:8]          # correlación en logs
    try:
        LOG.info("[%s] ↩️  POST /cargar-pedidos", trace_id)
        negocio = session.get("negocio")
        empresa = session.get("empresa")

        # ── 2.1 Leer payload ────────────────────────────────────────
        payload = request.get_json(silent=True) or {}
        data_inv = payload.get("inventario") or []
        data_mat = payload.get("materiales")
        LOG.info("[%s] Inventario filas: %d  |  Materiales filas: %s",
                 trace_id, len(data_inv), len(data_mat) if data_mat else "—")

        if not data_inv:
            raise ValueError("Inventario vacío o ausente.")
        if negocio != "nutresa" and data_mat is None:
            raise ValueError("Falta enviar el archivo de materiales.")

        # ── 2.2 DataFrame inventario ────────────────────────────────
        df_inv = pd.DataFrame(data_inv)[GEN_HEADERS["inventario"]].fillna("")
        df_inv["stock"] = df_inv["stock"].replace("", "0").astype(int)
        LOG.info("[%s] Inventario listo → %d filas", trace_id, len(df_inv))

        conn = conectar(); cur = conn.cursor()
        cur.execute(
            "CALL sp_etl_pedxrutaxprod_json(%s, %s);",
            (json.dumps(df_inv.to_dict("records")), empresa),
        )
        conn.commit()
        LOG.info("[%s] SP inventario ejecutado", trace_id)

        # ── 2.3 DataFrame materiales (opcional) ────────────────────
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
            LOG.info("[%s] SP materiales ejecutado", trace_id)

            cur.execute("SELECT fn_materiales_sin_definir(%s);", (empresa,))
            mis = json.loads(cur.fetchone()[0] or "[]")
            if mis:
                sin_def = ", ".join(f"{m['codigo_pro']}:{m['producto']}" for m in mis)
                raise ValueError(f"Materiales sin definir: {sin_def}")

        cur.close(); conn.close()

        # ── 2.4 Construir ZIP ───────────────────────────────────────
        LOG.info("[%s] Construyendo ZIP", trace_id)
        zip_buf = _build_zip(empresa, trace_id)
        nombre_zip = datetime.now().strftime("formatos_%Y%m%d_%H%M.zip")
        LOG.info("[%s] ✅ ZIP listo (%s)", trace_id, nombre_zip)

        return send_file(zip_buf, as_attachment=True,
                         download_name=nombre_zip, mimetype="application/zip")

    # —— Errores controlados  (negocio)  ——————————————
    except ValueError as ve:
        LOG.warning("[%s] ⚠️  %s", trace_id, ve)
        return jsonify(error=str(ve)), 400

    # —— Errores inesperados (programación/infra)  ——————————
    except Exception:
        tb = traceback.format_exc()
        LOG.error("[%s] 💥\n%s", trace_id, tb)
        return jsonify(error=tb), 500


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  3) Helper: ZIP con repartición + pedidos por ruta                   ║
# ╚══════════════════════════════════════════════════════════════════════╝
def _build_zip(empresa: int, trace_id: str) -> BytesIO:
    conn = conectar(); cur = conn.cursor()

    cur.execute("SELECT fn_obtener_reparticion_inventario_json(%s);", (empresa,))
    data_rep = json.loads(cur.fetchone()[0] or "[]")
    LOG.info("[%s] Repartición filas: %d", trace_id, len(data_rep))

    cur.execute("SELECT fn_obtener_pedidos_con_pedir_json(%s);", (empresa,))
    data_ped = json.loads(cur.fetchone()[0] or "[]")
    LOG.info("[%s] Pedidos filas: %d", trace_id, len(data_ped))

    cur.close(); conn.close()

    # Robustez plural → singular
    for lst in (data_rep, data_ped):
        for r in lst:
            if "pedidos" in r and "pedir" not in r:
                r["pedir"] = r.pop("pedidos")

    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        # 1) Repartición
        col_rep = ["ruta","codigo_pro","producto","cantidad","pedir","ped99","inv"]
        try:
            df_rep = pd.DataFrame(data_rep)[col_rep]
        except KeyError as e:
            raise ValueError(f"Columnas faltantes en repartición: {e}") from e

        buf = BytesIO()
        df_rep.to_excel(buf, sheet_name="Reparticion", index=False, engine="openpyxl")
        buf.seek(0); zf.writestr("reparticion_inventario.xlsx", buf.read())

        # 2) Pedidos por ruta
        for ruta in sorted({row["ruta"] for row in data_ped}):
            subset = [row for row in data_ped if row["ruta"] == ruta]
            try:
                df = pd.DataFrame(subset, columns=["codigo_pro","producto","pedir"])
            except KeyError as e:
                raise ValueError(f"Columnas faltantes en ruta {ruta}: {e}") from e
            df.insert(2, "UN", "UN")
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as xls:
                df.to_excel(xls, sheet_name=f"Ruta_{ruta}", index=False, startrow=3)
            buf.seek(0)
            zf.writestr(f"pedidos_ruta_{ruta}.xlsx", buf.read())

    zip_buf.seek(0)
    return zip_buf








