from flask import Flask, request, render_template, redirect, flash
import pandas as pd
import os
from sqlalchemy import create_engine

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "cambiar-esto")

# Cadena de conexión desde variable de entorno
DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

@app.route("/", methods=["GET", "POST"])
def upload():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or not file.filename.endswith((".xls", ".xlsx")):
            flash("Por favor, sube un archivo Excel válido.", "error")
            return redirect(request.url)

        # Lee todo en un DataFrame
        df = pd.read_excel(file, engine="openpyxl")
        # Ejemplo: insértalo en una tabla staging
        df.to_sql("staging_table", engine, if_exists="append", index=False)
        flash(f"{len(df)} filas importadas correctamente.", "success")
        return redirect(request.url)

    return render_template("upload.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
