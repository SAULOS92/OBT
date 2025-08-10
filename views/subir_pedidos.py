# views/subir_pedidos.py

import time
from flask import (
    Blueprint,
    session,
    flash,
    redirect,
    url_for,
    render_template,
    request,
)
from bs4 import BeautifulSoup
import pandas as pd, json, os, requests
from db import conectar
from views.auth import login_required


subir_pedidos_bp = Blueprint(
    "subir_pedidos", __name__, template_folder="../templates"
)


@subir_pedidos_bp.route("/subir-pedidos", methods=["GET", "POST"])
@login_required
def subir_pedidos():
    if request.method == "GET":
        return render_template("subir_pedidos.html")

    try:
        # 0. Empresa desde sesión
        empresa = session.get("empresa")
        if not empresa:
            flash('❌ Falta empresa en sesión', 'danger')
            return redirect(url_for('subir_pedidos.subir_pedidos'))

        # 1. Traer pedidos JSON
        conn = conectar()
        cur = conn.cursor()
        cur.execute("SELECT fn_obtener_pedidos_con_pedir_json(%s);", (empresa,))
        pedidos = json.loads(cur.fetchone()[0])
        cur.close(); conn.close()
        if not pedidos:
            flash(f'❌ No hay pedidos para "{empresa}"', 'danger')
            return redirect(url_for('subir_pedidos.subir_pedidos'))

        # 2. Solo la primera ruta (p.ej. “ruta1”)
        primera_ruta = next(iter({p['ruta'] for p in pedidos}))
        registros = [p for p in pedidos if p['ruta']==primera_ruta]

        # 3. Login
        LOGIN_URL = 'https://portal.gruponutresa.com/'
        EXCEL_URL = 'https://portal.gruponutresa.com/p/nuevo/pedido-masivo/excel'
        USR, PWD = '10318624', 'Abril2025*'
        sess = requests.Session()

        r = sess.get(LOGIN_URL)
        soup = BeautifulSoup(r.text,'html.parser')
        form = soup.find('form')
        action = form.get('action') or LOGIN_URL
        payload = {}
        for inp in form.find_all('input'):
            name = inp.get('name')
            if not name: continue
            tp = inp.get('type','').lower()
            if tp=='password': payload[name]=PWD
            elif tp in('text','email') and 'user' in name.lower(): payload[name]=USR
            else: payload[name]=inp.get('value','')
        sess.post(action, data=payload, timeout=30)

        # 4. Generar Excel de la ruta1
        df = pd.DataFrame(registros)[['codigo_pro','producto','pedir']]
        df.insert(2,'UN','UN')
        fichero = f"{primera_ruta}.xlsx"
        df.to_excel(fichero, sheet_name='Pedidos', startrow=4, index=False, engine='openpyxl')

        # 5. Subida con polling para esperar el formulario
        def espera_form(url, comprueba, timeout=30, intervalo=1):
            fin = time.time() + timeout
            while time.time()<fin:
                resp = sess.get(url)
                sopa = BeautifulSoup(resp.text,'html.parser')
                if comprueba(sopa):
                    return sopa
                time.sleep(intervalo)
            raise TimeoutError(f"No apareció el elemento en {timeout}s")

        # 5.1 Esperar a que el form de EXCEL_URL esté listo
        soup2 = espera_form(EXCEL_URL,
            lambda s: s.find('form'))
        form2 = soup2.find('form')
        action2 = form2.get('action')

        # recolectar hidden y seleccionar templateType=Standard
        data2 = {i['name']:i.get('value','')
                 for i in form2.find_all('input',{'type':'hidden'})}
        data2['templateType'] = 'Standard'

        file_field = form2.find('input',{'type':'file'})['name']
        with open(fichero,'rb') as f:
            files = {file_field:(os.path.basename(fichero),f,
                                 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
            resp_up = sess.post(action2, data=data2, files=files, timeout=60)
        os.remove(fichero)
        resp_up.raise_for_status()

        # 6. Si aparece “Agregar al carrito”, envía ese form
        soup3 = BeautifulSoup(resp_up.text,'html.parser')
        btn = soup3.find('button', string=lambda t: t and 'Agregar al carrito' in t)
        if btn:
            form3 = btn.find_parent('form')
            act3  = form3['action']
            data3 = {i['name']:i.get('value','') for i in form3.find_all('input') if i.get('name')}
            sess.post(act3, data=data3, timeout=30)

        # 7. Ir al carrito/resumen y confirmar
        resumen = sess.get('https://portal.gruponutresa.com/carrito/resumen')
        soup4   = BeautifulSoup(resumen.text,'html.parser')
        form4   = soup4.find('form')  # ajusta el selector si es necesario
        act4    = form4['action']
        data4   = {i['name']:i.get('value','') for i in form4.find_all('input') if i.get('name')}
        data4['ordenCompra'] = 'carro1'
        resp4   = sess.post(act4, data=data4, timeout=30)
        resp4.raise_for_status()

        # 8. Confirmar pedido
        soup5  = BeautifulSoup(resp4.text,'html.parser')
        btn_ok = soup5.find('button', string=lambda t: t and 'Confirmar pedido' in t)
        if btn_ok:
            frm_ok = btn_ok.find_parent('form')
            act_ok = frm_ok['action']
            data_ok= {i['name']:i.get('value','') for i in frm_ok.find_all('input') if i.get('name')}
            sess.post(act_ok, data=data_ok, timeout=30)

        flash('✅ Pedido ruta1 subido y confirmado', 'success')

    except Exception as e:
        flash(f'❌ Error: {e}', 'danger')

    return redirect(url_for('subir_pedidos.subir_pedidos'))

