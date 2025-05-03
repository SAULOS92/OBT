import os
import pandas as pd
from io import BytesIO
from datetime import datetime
from flask import (
    Blueprint, render_template, request,
    flash, redirect, url_for, send_file,
    current_app
)

consolidar_bp = Blueprint(
    "consolidar_compras", __name__,
    template_folder="../templates"
)

# --- (toda la configuración COLUMN_CONFIG y ECOM_COLUMN_SPEC queda igual) ---

# Ruta principal
@consolidar_bp.route("/consolidar-compras", methods=["GET", "POST"])
def consolidar_compras_index():
    download_filename = None

    if request.method == "POST":
        fmt = request.form.get("format")
        f   = request.files.get("archivo")
        if not f or fmt not in ("celluweb", "ecom"):
            flash("Sube un Excel y elige un formato válido.", "error")
            return redirect(url_for(".consolidar_compras_index"))

        try:
            # --- (todo tu procesamiento: lectura, validación, agrupación, ECOM vs CELUWEB) ---
            # Al final, df_out queda listo y filename definido:
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df_out.to_excel(writer, index=False)
            buf.seek(0)

            hoy = datetime.now().strftime("%Y%m%d")
            if fmt == "celluweb":
                filename = f"consolidado_celuweb_{hoy}.xlsx"
            else:
                filename = f"consolidado_ecom_{hoy}.xlsx"

            # --- Guardar en tmp para descarga posterior ---
            tmp_dir = os.path.join(current_app.root_path, "tmp")
            os.makedirs(tmp_dir, exist_ok=True)
            path = os.path.join(tmp_dir, filename)
            with open(path, "wb") as out:
                out.write(buf.getvalue())

            download_filename = filename
            flash("Consolidado generado con éxito.", "success")

        except Exception as e:
            flash(f"Error procesando informe {fmt}: {e}", "error")

    return render_template(
        "consolidar_compras.html",
        download_filename=download_filename
    )


# Ruta para descargar el archivo ya generado
@consolidar_bp.route("/consolidar-compras/download/<filename>")
def descargar_archivo_file(filename):
    tmp_dir = os.path.join(current_app.root_path, "tmp")
    path    = os.path.join(tmp_dir, filename)
    if not os.path.exists(path):
        flash("El archivo ya no está disponible.", "error")
        return redirect(url_for("consolidar_compras.consolidar_compras_index"))
    return send_file(
        path,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )









