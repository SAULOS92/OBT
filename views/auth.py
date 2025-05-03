from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from functools import wraps
from conectar import conectar  # tu función para obtener conexión

auth_bp = Blueprint('auth', __name__)

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email    = request.form['email']
        password = request.form['password']

        conn = conectar()
        cur  = conn.cursor()
        cur.execute("""
            SELECT id, email
            FROM users
            WHERE email = %s
              AND password_hash = crypt(%s, password_hash)
        """, (email, password))
        fila = cur.fetchone()
        cur.close()
        conn.close()

        if fila:
            # Extraer 'nombre_de_empresa' de facturacion@empresa.com
            empresa = email.split('@')[1].split('.')[0]

            session.clear()
            session['user_id'] = fila[0]
            session['empresa'] = empresa
            return redirect(url_for('upload.upload_index'))
        else:
            flash('Email o contraseña inválidos.', 'error')

    return render_template('login.html')

