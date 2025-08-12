import gzip, json
import msgpack
from datetime import datetime
from io import BytesIO
import pandas as pd
import time
from flask import (
    Blueprint, render_template, request,
    send_file, session, jsonify
)
from db import conectar
from views.auth import login_required

upload_bp = Blueprint("upload", __name__, template_folder="../templates")


def _get_json_gzip_aware():
    raw = request.get_data()
    if request.headers.get("Content-Encoding") == "gzip":
        raw = gzip.decompress(raw)
    return msgpack.unpackb(raw, raw=False)


@upload_bp.route("/", methods=["GET", "POST"])
@upload_bp.route("/cargar-pedidos", methods=["GET", "POST"])
@login_required
def upload_index():
    empresa = session.get("empresa")

    if request.method == "POST":
        try:
            with conectar() as conn:
                with conn.cursor() as cur:
                    t0 = time.perf_counter()
                    # ---- 1) JSON proveniente del frontend --------------------
                    payload = _get_json_gzip_aware()
                    pedidos = payload.get("pedidos", [])
                    rutas = payload.get("rutas")
                    p_dia = request.args.get("dia", "").strip()

                    # ---- 2) Procedimiento almacenado -------------------------
                    cur.execute(
                        "CALL etl_cargar_pedidos_y_rutas_masivo(%s, %s, %s, %s);",
                        (json.dumps(pedidos), json.dumps(rutas), p_dia, empresa)
                    )
                    cur.execute("SELECT fn_obtener_resumen_pedidos(%s);", (empresa,))
                    raw = cur.fetchone()[0]
                    conn.commit()

                    elapsed = time.perf_counter() - t0
                    print(f"[INFO] SP etl_cargar_pedidos_y_rutas_masivo demoró {elapsed:.2f}s "
                          f"(pedidos={len(pedidos)}, rutas={len(rutas)}, día={p_dia}, emp={empresa})")

            # ---- 3) Obtener resumen en JSON --------------------------
            data_res = json.loads(raw) if isinstance(raw, str) else (raw or [])
            cols = ["bd", "codigo_cli", "nombre", "barrio", "ciudad", "asesor", "total_pedidos", "valor", "ruta"]
            df_res = pd.DataFrame(data_res, columns=cols)

            # ---- 4) Exportar a Excel
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df_res.to_excel(writer, sheet_name="ResumenPedidos", index=False)
            buf.seek(0)
            hoy = datetime.now().strftime("%Y%m%d_%H%M")
            nombre_xlsx = f"ResumenPedidos_{hoy}.xlsx"

            return send_file(
                buf,
                as_attachment=True,
                download_name=nombre_xlsx,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            # Devuelve JSON para que el front lo capture
            error_msg = getattr(e, 'diag', None).message_primary if getattr(e, 'diag', None) else str(e)
            return jsonify(error=error_msg), 400

    # GET: muestra formulario
    return render_template("upload.html")





