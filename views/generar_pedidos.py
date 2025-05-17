"""
Blueprint: generar_pedidos_bp
Responsabilidades
──────────────────
1.   GET /generar-pedidos
     ▸ Devuelve la plantilla con el formulario (validación en el navegador).

2.   POST /cargar-pedidos          (llamada desde fetch del frontend)
     ▸ Recibe JSON con inventario (oblig.) y materiales (opcional).
     ▸ Ejecuta los SP que cargan/transforman la info.
     ▸ Si hay materiales sin definir      → HTTP 400 + JSON {error:"…"}
     ▸ Si todo va bien                    → HTTP 200 + ZIP con repartición
"""

import json
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

# ──────────────────────── Constantes de encabezados ────────────────────────
GEN_HEADERS = {
    "materiales": ["pro_codigo", "particion", "pq_x_caja"],
    "inventario": ["codigo", "stock"],
}

# ─────────────────────────── Blueprint ──────────────────────────────────────
generar_pedidos_bp = Blueprint(
    "generar_pedidos", __name__,
    template_folder="../templates",
)

# ╔══════════════════════════════════════════════════════════════════════╗
# ║ 1)  Renderizar página (solicitud GET)                               ║
# ╚══════════════════════════════════════════════════════════════════════╝
@generar_pedidos_bp.route("/generar-pedidos", methods=["GET"])
@login_required
def generar_pedidos_index():
    """Muestra la página principal. No maneja subida de archivos."""
    return render_template(
        "generar_pedidos.html",
        negocio=session.get("negocio"),
    )

# ╔══════════════════════════════════════════════════════════════════════╗
# ║ 2)  Endpoint consumido por fetch()                                  ║
# ║     Recibe JSON ➜ devuelve ZIP o error JSON                         ║
# ╚══════════════════════════════════════════════════════════════════════╝
@generar_pedidos_bp.route("/cargar-pedidos", methods=["POST"])
@login_required
def cargar_pedidos():
    negocio = session.get("negocio")
    empresa = session.get("empresa")

    # ───── 2.1  Parseo / validación básica de payload ────────────────
    payload = request.get_json(silent=True) or {}
    data_inv: list[dict] = payload.get("inventario") or []
    data_mat: list[dict] | None = payload.get("materiales")

    if not data_inv:
        return jsonify(error="Inventario vacío o ausente"), 400
    if negocio != "nutresa" and data_mat is None:
        return jsonify(error="Falta enviar el archivo de materiales"), 400

    # Normaliza tipos para que el SP no falle (frontend ya hizo lo suyo)
    df_inv = pd.DataFrame(data_inv)[GEN_HEADERS["inventario"]].fillna("")
    df_inv["stock"] = df_inv["stock"].replace("", "0").astype(int)

    conn = conectar()
    cur = conn.cursor()

    # ───── 2.2  Cargar inventario (obligatorio) ───────────────────────
    cur.execute(
        "CALL sp_etl_pedxrutaxprod_json(%s, %s);",
        (json.dumps(df_inv.to_dict(orient="records")), empresa),
    )
    conn.commit()

    # ───── 2.3  Cargar materiales (según negocio) ─────────────────────
    if data_mat is not None and negocio != "nutresa":
        df_mat = pd.DataFrame(data_mat)[GEN_HEADERS["materiales"]].fillna("")

        # «'' → 1 → int» para las columnas numéricas
        for col in ["particion", "pq_x_caja"]:
            df_mat[col] = (
                df_mat[col].astype(str).str.strip().replace("", "1").astype(float).astype(int)
            )

        cur.execute(
            "CALL sp_cargar_materiales(%s, %s);",
            (json.dumps(df_mat.to_dict(orient="records")), empresa),
        )
        conn.commit()

        # ───── 2.3.a  Verifica materiales sin definir ───────────────
        cur.execute("SELECT fn_materiales_sin_definir(%s);", (empresa,))
        raw = cur.fetchone()[0]
        mis = json.loads(raw) if isinstance(raw, str) else (raw or [])
        if mis:
            detalles = ", ".join(f"{m['codigo_pro']}:{m['producto']}" for m in mis)
            cur.close(); conn.close()
            return jsonify(error=f"Materiales sin definir: {detalles}"), 400

    cur.close(); conn.close()

    # ───── 2.4  Generar ZIP de salida (repartición + pedidos ruta) ─────
    zip_buf = _build_zip(empresa)

    hoy = datetime.now().strftime("%Y%m%d_%H%M")
    nombre_zip = f"formatos_{hoy}.zip"

    return send_file(
        zip_buf,
        as_attachment=True,
        download_name=nombre_zip,
        mimetype="application/zip",
    )

# ╔══════════════════════════════════════════════════════════════════════╗
# ║ 3)  Helper: reúne info y construye ZIP en memoria                   ║
# ╚══════════════════════════════════════════════════════════════════════╝
def _build_zip(empresa: int) -> BytesIO:
    """Devuelve BytesIO con el ZIP listo para enviar."""
    conn = conectar(); cur = conn.cursor()

    # a) Repartición inventario
    cur.execute("SELECT fn_obtener_reparticion_inventario_json(%s);", (empresa,))
    raw_rep = cur.fetchone()[0]
    data_rep = json.loads(raw_rep) if isinstance(raw_rep, str) else (raw_rep or [])

    # b) Pedidos por ruta
    cur.execute("SELECT fn_obtener_pedidos_con_pedir_json(%s);", (empresa,))
    raw_ped = cur.fetchone()[0]
    data_ped = json.loads(raw_ped) if isinstance(raw_ped, str) else (raw_ped or [])

    cur.close(); conn.close()

    # ─── Construir ZIP ────────────────────────────────────────────────
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        # 1. Hoja Repartición
        col_rep = ["ruta", "codigo_pro", "producto", "cantidad",
                   "pedir", "ped99", "inv"]
        df_rep = pd.DataFrame(data_rep)[col_rep]
        buf = BytesIO()
        df_rep.to_excel(buf, sheet_name="Reparticion", index=False, engine="openpyxl")
        buf.seek(0)
        zf.writestr("reparticion_inventario.xlsx", buf.read())

        # 2. Un XLSX por ruta
        rutas = sorted({row["ruta"] for row in data_ped})
        for ruta in rutas:
            subset = [d for d in data_ped if d["ruta"] == ruta]
            df = pd.DataFrame(subset, columns=["codigo_pro", "producto", "pedir"])
            df.insert(2, "UN", "UN")  # Columna fija

            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as xls:
                df.to_excel(xls, sheet_name=f"Ruta_{ruta}",
                            index=False, startrow=3)  # Datos desde fila 4
            buf.seek(0)
            zf.writestr(f"pedidos_ruta_{ruta}.xlsx", buf.read())

    zip_buf.seek(0)
    return zip_buf







