from flask import Blueprint, render_template, session, send_file, flash, redirect, url_for
import pandas as pd
import io
from app import db  # o tu conexión específica

exportar_bp = Blueprint('exportar', __name__)

@exportar_bp.route('/exportar', methods=['GET'])
def exportar_view():
    return render_template('exportar_excel.html')

@exportar_bp.route('/exportar/descargar', methods=['POST'])
def descargar_excel():
    empresa = session.get('empresa')
    if not empresa:
        flash('Empresa no definida en la sesión', 'danger')
        return redirect(url_for('exportar.exportar_view'))

    try:
        query1 = f"SELECT * FROM PEDXCLIXPROD WHERE bd = %s"
        df1 = pd.read_sql(query1, db.engine, params=(empresa,))

        query2 = f"SELECT * FROM pedxrutaxprod WHERE bd = %s"
        df2 = pd.read_sql(query2, db.engine, params=(empresa,))

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df1.to_excel(writer, sheet_name='PEDXCLIXPROD', index=False)
            df2.to_excel(writer, sheet_name='pedxrutaxprod', index=False)
        output.seek(0)

        return send_file(output,
                         download_name='exportado.xlsx',
                         as_attachment=True,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    except Exception as e:
        flash(f'Error al exportar: {str(e)}', 'danger')
        return redirect(url_for('exportar.exportar_view'))
