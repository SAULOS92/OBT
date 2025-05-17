import json
from datetime import datetime
from io import BytesIO
import pandas as pd
from flask import (
    Blueprint, render_template, request,
    flash, redirect, url_for, send_file, session
)
from db import conectar
from views.auth import login_required

upload_bp = Blueprint("upload", __name__, template_folder="../templates")

@upload_bp.route("/", methods=["GET","POST"])
@upload_bp.route("/cargar-pedidos", methods=["GET","POST"])
@login_required
def upload_index():
    empresa = session.get("empresa")
    mostrar_descarga = bool(request.args.get("descarga", default=0, type=int))

    if request.method == "POST":
        try:
            # Recibe JSON preparado en el frontend: pedidos y rutas
            data = request.get_json(force=True)
            pedidos = data["pedidos"]
            rutas = data["rutas"]
            # Día seleccionado como parámetro de consulta
            p_dia = request.args.get("dia", "").strip()

            # Llamada directa al stored procedure con ambos JSON y el día
            conn = conectar()
            cur = conn.cursor()
            cur.execute(
                "CALL etl_cargar_pedidos_y_rutas_masivo(%s, %s, %s, %s);",
                (
                    json.dumps(pedidos),
                    json.dumps(rutas),
                    p_dia,
                    empresa
                )
            )
            conn.commit()
            cur.close()
            conn.close()

            flash("¡Carga masiva exitosa!", "success")
            return redirect(url_for("upload.upload_index", descarga=1))

        except Exception as e:
            flash(f"Error inesperado: {e}", "error")
            return redirect(url_for("upload.upload_index"))

    return render_template(
        "upload.html",
        mostrar_descarga=mostrar_descarga
    )

@upload_bp.route("/cargar-pedidos/descargar-resumen", methods=["GET"])
@login_required
def descargar_resumen():
    empresa = session.get("empresa")
    conn = conectar()
    cur = conn.cursor()
    cur.execute("SELECT fn_obtener_resumen_pedidos(%s);", (empresa,))
    raw = cur.fetchone()[0]
    cur.close()
    conn.close()

    data = json.loads(raw) if isinstance(raw, str) else (raw or [])
    cols = ["bd","codigo_cli","nombre","barrio","ciudad","asesor","total_pedidos","valor","ruta"]
    df_res = pd.DataFrame(data, columns=cols)

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


