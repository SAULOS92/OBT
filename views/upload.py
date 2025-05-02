# views/upload.py

import json
import pandas as pd
from flask import Blueprint, render_template, request, flash, redirect, url_for, send_file
from db import conectar

upload_bp = Blueprint("upload", __name__, template_folder="../templates")

# Columnas internas que espera el SP
PED_HEADERS = [
    "numero_pedido", "hora", "cliente", "nombre", "barrio", "ciudad",
    "asesor", "codigo_pro", "producto", "cantidad", "valor",
    "tipo_pro", "estado"
]
RUT_HEADERS = ["codigo_cliente", "codigo_ruta"]
DIAS_VALIDOS = {"LU", "MA", "MI", "JU", "VI", "SA", "DO"}

# Mapas de sinónimos → nombre interno
PED_COL_MAP = {
    "numero_pedido": ["numero_pedido", "Pedido"],
    "hora":          ["hora", "Hora"],
    "cliente":       ["cliente", "Cliente"],
    "nombre":        ["nombre", "R. Social"],
    "barrio":        ["barrio", "Barrio"],
    "ciudad":        ["ciudad", "Ciudad"],
    "asesor":        ["asesor", "Asesor"],
    "codigo_pro":    ["codigo_pro", "Cod.Prod"],  # seguir como texto
    "producto":      ["producto", "Producto"],
    "cantidad":      ["cantidad", "Cantidad"],
    "valor":         ["valor", "Total"],
    "tipo_pro":      ["tipo_pro", "Tip Pro"],
    "estado":        ["estado", "Estado"]
}
RUT_COL_MAP = {
    "codigo_cliente": ["Cod. Cliente"],
    "codigo_ruta":    ["Ruta"]
}

def normalize_cols(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    """Renombra columnas según col_map (solo mapeo de sinónimos)."""
    inv = {}
    for internal, syns in col_map.items():
        for s in syns:
            inv[s.strip().lower()] = internal

    to_rename = {
        col: inv[col.strip().lower()]
        for col in df.columns
        if col.strip().lower() in inv
    }
    return df.rename(columns=to_rename)


@upload_bp.route("/", methods=["GET", "POST"])
@upload_bp.route("/cargar-pedidos", methods=["GET", "POST"])
def upload_index():
    if request.method == "POST":
        # 1) Recoger archivos y día
        f_ped = request.files.get("pedidos")
        f_rut = request.files.get("rutas")
        p_dia = request.form.get("dia", "").strip()
        if not f_ped or not f_rut or p_dia not in DIAS_VALIDOS:
            flash("Sube ambos archivos y selecciona un día válido.", "error")
            return redirect(request.url)

        # 2) Leer ambos Excel COMO TEXTO y rellenar vacíos con cadena
        try:
            df_ped = pd.read_excel(f_ped, engine="openpyxl", dtype=str).fillna("")
            df_rut = pd.read_excel(f_rut, engine="openpyxl", dtype=str).fillna("")
        except Exception as e:
            flash(f"Error leyendo los Excel: {e}", "error")
            return redirect(request.url)

        # 3) Normalizar nombres con COL_MAP
        df_ped = normalize_cols(df_ped, PED_COL_MAP)
        df_rut = normalize_cols(df_rut, RUT_COL_MAP)

        # 4) Validar encabezados faltantes
        falt_ped = [h for h in PED_HEADERS if h not in df_ped.columns]
        if falt_ped:
            flash(f"Faltan columnas en Pedidos: {falt_ped}", "error")
            return redirect(request.url)
        falt_rut = [h for h in RUT_HEADERS if h not in df_rut.columns]
        if falt_rut:
            flash(f"Faltan columnas en Rutas: {falt_rut}", "error")
            return redirect(request.url)

        # 5) Detectar duplicados
        dup_p = df_ped.columns[df_ped.columns.duplicated()].unique().tolist()
        if dup_p:
            flash(f"Encabezados duplicados en Pedidos: {dup_p}", "error")
            return redirect(request.url)
        dup_r = df_rut.columns[df_rut.columns.duplicated()].unique().tolist()
        if dup_r:
            flash(f"Encabezados duplicados en Rutas: {dup_r}", "error")
            return redirect(request.url)

        # 6) Rellenar nombre, barrio y ciudad ausentes usando el grupo 'cliente'
        for col in ("nombre", "barrio", "ciudad"):
            df_ped[col] = (
                df_ped.groupby("cliente")[col]
                .apply(lambda g: g.replace("", pd.NA).ffill().bfill())
                .fillna("")
            )

        # 7) Forzar tipos numéricos SOLO donde corresponde y poner 0 en cadenas vacías
        try:
            df_ped["cantidad"] = df_ped["cantidad"].replace("", "0").astype(int)
            df_ped["valor"]    = df_ped["valor"].replace("", "0").astype(float)
            # df_ped["codigo_pro"] queda como texto
        except ValueError as e:
            flash(f"Formato numérico inválido: {e}", "error")
            return redirect(request.url)

        # 8) Serializar JSON para el SP
        pedidos = df_ped[PED_HEADERS].to_dict(orient="records")
        rutas   = df_rut[RUT_HEADERS].to_dict(orient="records")

        # 9) Ejecutar el procedimiento almacenado
        try:
            conn = conectar()
            cur = conn.cursor()
            cur.execute(
                "CALL etl_cargar_pedidos_y_rutas_masivo(%s, %s, %s);",
                (json.dumps(pedidos), json.dumps(rutas), p_dia)
            )
            conn.commit()
        except Exception as e:
            flash(f"Error en ETL: {e}", "error")
            cur.close(); conn.close()
            return redirect(request.url)
        finally:
            cur.close(); conn.close()

        # 10) Obtener resumen y devolver Excel
        try:
            conn = conectar()
            cur = conn.cursor()
            cur.execute("SELECT fn_obtener_resumen_pedidos();")
            raw = cur.fetchone()[0]
            cur.close(); conn.close()
            data = json.loads(raw) if isinstance(raw, str) else (raw or [])
        except Exception as e:
            flash(f"Error al generar resumen: {e}", "error")
            return redirect(request.url)

        cols = ["codigo_cli","nombre","barrio","ciudad","asesor","total_pedidos","ruta"]
        df_res = pd.DataFrame(data, columns=cols)

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_res.to_excel(writer, sheet_name="ResumenPedidos", index=False)
        output.seek(0)

        return send_file(
            output,
            as_attachment=True,
            download_name="resumen_pedidos.xlsx",
            mimetype=(
                "application/vnd.openxmlformats-officedocument"
                "-spreadsheetml.sheet"
            )
        )

    # GET → formulario
    return render_template("upload.html")


