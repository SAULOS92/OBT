from datetime import date
from functools import wraps
from flask import Blueprint, render_template, request, redirect, url_for, flash, session

from db import conectar

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

        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, email, negocio, membership_end
                    FROM users
                    WHERE email = %s
                      AND password_hash = crypt(%s, password_hash)
                    """,
                    (email, password),
                )
                fila = cur.fetchone()
        

        if fila:
            membership_end = fila[3]
            if membership_end and membership_end < date.today():
                flash(
                    'Membresía finalizada. Por favor contacte al administrador del sistema para ampliarla.',
                    'error',
                )
                return render_template('login.html')

            empresa = email.split('@')[1].split('.')[0]
            session.clear()
            session['user_id'] = fila[0]
            session['empresa'] = empresa
            session['negocio'] = fila[2]
            session['membership_end'] = membership_end.isoformat() if membership_end else None
            return redirect(url_for('upload.upload_index'))
        else:
            flash('Email o contraseña inválidos.', 'error')


    return render_template('login.html')

@auth_bp.route('/logout')
def logout():
    """Limpia la sesión y redirige al login."""
    session.clear()
    flash('Has cerrado sesión.', 'success')
    return redirect(url_for('auth.login'))

