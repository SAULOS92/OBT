from datetime import datetime
from functools import wraps

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from db import conectar
from views.auth import login_required

ADMIN_EMAIL = "saulosorioh@gmail.com"

admin_bp = Blueprint("admin", __name__, template_folder="../templates")


def admin_required(view_func):
    @wraps(view_func)
    @login_required
    def wrapper(*args, **kwargs):
        if session.get("email") != ADMIN_EMAIL:
            flash("No tienes permisos para acceder al panel de administración.", "error")
            return redirect(url_for("upload.upload_index"))
        return view_func(*args, **kwargs)

    return wrapper


@admin_bp.route("/admin", methods=["GET", "POST"])
@admin_required
def admin_dashboard():
    if request.method == "POST":
        action = request.form.get("action", "").strip()

        if action == "create_user":
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "")
            nombre = request.form.get("nombre", "").strip()
            documento = request.form.get("documento", "").strip()
            contacto = request.form.get("contacto", "").strip()
            negocio = request.form.get("negocio", "").strip().lower()
            membership_start = request.form.get("membership_start", "").strip()
            membership_end = request.form.get("membership_end", "").strip()

            if not email or not password or not negocio:
                flash("Email, contraseña y negocio son obligatorios para crear usuarios.", "error")
                return redirect(url_for("admin.admin_dashboard"))

            try:
                membership_start_date = (
                    datetime.strptime(membership_start, "%Y-%m-%d").date()
                    if membership_start else datetime.now().date()
                )
            except ValueError:
                flash("La fecha inicial no tiene formato válido (YYYY-MM-DD).", "error")
                return redirect(url_for("admin.admin_dashboard"))

            membership_end_date = None
            if membership_end:
                try:
                    membership_end_date = datetime.strptime(membership_end, "%Y-%m-%d").date()
                except ValueError:
                    flash("La fecha final no tiene formato válido (YYYY-MM-DD).", "error")
                    return redirect(url_for("admin.admin_dashboard"))

            try:
                with conectar() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1 FROM users WHERE email = %s", (email,))
                        if cur.fetchone():
                            flash("Ya existe un usuario con ese email.", "error")
                            return redirect(url_for("admin.admin_dashboard"))

                        cur.execute(
                            """
                            INSERT INTO users (
                                email,
                                nombre,
                                documento,
                                contacto,
                                password_hash,
                                membership_start,
                                membership_end,
                                negocio
                            )
                            VALUES (%s, %s, %s, %s, crypt(%s, gen_salt('bf')), %s, %s, %s)
                            """,
                            (
                                email,
                                nombre or None,
                                documento or None,
                                contacto or None,
                                password,
                                membership_start_date,
                                membership_end_date,
                                negocio,
                            ),
                        )
                        conn.commit()
                flash("Usuario creado correctamente.", "success")
            except Exception as exc:
                flash(f"No fue posible crear el usuario: {exc}", "error")

            return redirect(url_for("admin.admin_dashboard"))

        if action == "change_password":
            email = request.form.get("email", "").strip().lower()
            new_password = request.form.get("new_password", "")

            if not email or not new_password:
                flash("Debes indicar el email y la nueva contraseña.", "error")
                return redirect(url_for("admin.admin_dashboard"))

            try:
                with conectar() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE users
                            SET password_hash = crypt(%s, gen_salt('bf'))
                            WHERE email = %s
                            """,
                            (new_password, email),
                        )
                        if cur.rowcount == 0:
                            flash("No existe un usuario con ese email.", "error")
                            return redirect(url_for("admin.admin_dashboard"))
                        conn.commit()
                flash("Contraseña actualizada correctamente.", "success")
            except Exception as exc:
                flash(f"No fue posible actualizar la contraseña: {exc}", "error")

            return redirect(url_for("admin.admin_dashboard"))

    return render_template("admin.html", admin_email=ADMIN_EMAIL)
