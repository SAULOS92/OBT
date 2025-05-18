import json, traceback, logging, uuid
from io import BytesIO
import zipfile
from datetime import datetime

import pandas as pd
from flask import Blueprint, render_template, request, jsonify, send_file, session
from views.auth import login_required
from db import conectar

generar_pedidos_bp = Blueprint("generar_pedidos", __name__, template_folder="../templates")

@generar_pedidos_bp.route("/generar-pedidos", methods=["GET"])
@login_required
def generar_pedidos_index():
    return render_template("generar_pedidos.html", negocio=session.get("negocio"))

@generar_pedidos_bp.route("/generar-pedidos", methods=["POST"])
@login_required
def cargar_pedidos():
    trace_id = uuid.uuid4().hex[:8]
    try:
        negocio = session.get("negocio"); empresa = session.get("empresa")
        payload  = request.get_json(silent=True) or {}
        data_inv = payload.get("inventario") or []
        data_mat = payload.get("materiales")        
        


        conn = conectar(); cur = conn.cursor()
        if data_mat and negocio != "nutresa":       
            
            cur.execute("CALL sp_cargar_materiales(%s, %s);",
                        (json.dumps(data_mat), empresa))
            
            cur.execute("SELECT fn_materiales_sin_definir(%s);", (empresa,))
            sin_def = json.loads(cur.fetchone()[0] or "[]")
            if sin_def:
                listado = ", ".join(f"{m['codigo_pro']}:{m['producto']}" for m in sin_def)
                raise ValueError(f"Materiales sin definir: {listado}")
            
            conn.commit()
            cur.close(); conn.close()
        conn = conectar(); cur = conn.cursor()

        cur.execute("CALL sp_etl_pedxrutaxprod_json(%s, %s);",
                    (json.dumps(data_inv), empresa))
        conn.commit()
        cur.close(); conn.close()

        zip_buf = _build_zip(empresa)
        nombre  = datetime.now().strftime("formatos_%Y%m%d_%H%M.zip")
        return send_file(zip_buf, as_attachment=True,
                         download_name=nombre, mimetype="application/zip")

    except ValueError as ve:        
        return jsonify(error=str(ve)), 400
    except Exception:
        tb = traceback.format_exc()        
        return jsonify(error=tb), 500


def _build_zip(empresa: int) -> BytesIO:
    conn = conectar(); cur = conn.cursor()
    cur.execute("SELECT fn_obtener_reparticion_inventario_json(%s);", (empresa,))
    data_rep = json.loads(cur.fetchone()[0] or "[]")
    cur.execute("SELECT fn_obtener_pedidos_con_pedir_json(%s);", (empresa,))
    data_ped = json.loads(cur.fetchone()[0] or "[]")
    cur.close(); conn.close()    
    

    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        # Repartición
        df_rep = pd.DataFrame(data_rep)[
            ["ruta", "codigo_pro", "producto", "cantidad", "pedir", "ped99", "inv"]
        ]
        buf = BytesIO()
        df_rep.to_excel(buf, index=False, sheet_name="Reparticion", engine="openpyxl")
        buf.seek(0); zf.writestr("reparticion_inventario.xlsx", buf.read())

        # Un .xlsx por ruta
        for ruta in sorted({r["ruta"] for r in data_ped}):
            df = pd.DataFrame(
                [r for r in data_ped if r["ruta"] == ruta],
                columns=["codigo_pro", "producto", "pedir"])
            df.insert(2, "UN", "UN")
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as xlsx:
                df.to_excel(xlsx, index=False, startrow=3, sheet_name=f"Ruta_{ruta}")
            buf.seek(0)
            zf.writestr(f"pedidos_ruta_{ruta}.xlsx", buf.read())

    zip_buf.seek(0)
    return zip_buf







