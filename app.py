import os, json
import pandas as pd
import psycopg2
from flask import Flask, render_template, request, redirect, flash, url_for

app = Flask(__name__, static_folder="static")
app.secret_key = os.getenv("SECRET_KEY", "cámbiala_en_producción")

# Cadena de conexión que Render expone en la variable DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")

# Encabezados esperados
PED_HEADERS = [
    "numero_pedido","hora","cliente","nombre","barrio","ciudad",
    "asesor","codigo_pro","producto","cantidad","valor","tipo","estado"
]
RUT_HEADERS = ["cliente","dia","codigo_ruta"]

def conectar():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

@app.route("/", methods=["GET","POST"])
def index():
    if request.method == "POST":
        # Archivos y día
        f_ped = request.files.get("pedidos")
        f_rut = request.files.get("rutas")
        p_dia = request.form.get("dia","").strip()
        if not (f_ped and f_rut and p_dia):
            flash("Todos los campos son obligatorios.", "error")
            return redirect(url_for("index"))

        # 1) Leemos con pandas
        try:
            df_ped = pd.read_excel(f_ped, engine="openpyxl")
            df_rut = pd.read_excel(f_rut, engine="openpyxl")
        except Exception as e:
            flash(f"Error leyendo los Excel: {e}", "error")
            return redirect(url_for("index"))

        # 2) Validamos encabezados
        if list(df_ped.columns) != PED_HEADERS:
            flash(f"Encabezados de pedidos inválidos. Deben ser: {PED_HEADERS}", "error")
            return redirect(url_for("index"))
        if list(df_rut.columns) != RUT_HEADERS:
            flash(f"Encabezados de rutas inválidos. Deben ser: {RUT_HEADERS}", "error")
            return redirect(url_for("index"))

        # 3) Convertimos a listas de dicts
        p_pedidos = df_ped.to_dict(orient="records")
        p_rutas   = df_rut.to_dict(orient="records")

        # 4) Llamamos al procedimiento
        try:
            conn = conectar()
            cur = conn.cursor()
            cur.execute(
                "CALL etl_cargar_pedidos_y_rutas_masivo(%s, %s, %s);",
                (json.dumps(p_pedidos), json.dumps(p_rutas), p_dia)
            )
            conn.commit()
            cur.close()
            conn.close()
            flash("¡Carga masiva exitosa!", "success")
        except Exception as e:
            flash(f"Error al ejecutar el ETL: {e}", "error")

        return redirect(url_for("index"))

    # GET ➞ formulario
    return render_template("upload.html")

if __name__ == "__main__":
    # en local: export FLASK_ENV=development
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)

