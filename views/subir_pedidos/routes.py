"""Rutas HTTP para gestionar la vista de subir pedidos."""

import json
import traceback
from typing import List

from flask import jsonify, render_template, request, session

from db import conectar
from views.auth import login_required

from . import subir_pedidos_bp
from .automation import (
    cargar_pedido_masivo_excel,
    crear_archivo_pedido_masivo,
    iniciar_navegador,
    login_portal_grupo_nutresa,
)
from .vehiculos import add_ruta, delete_ruta, ensure_table, get_vehiculos, upsert_vehiculo


def log_pedidos_rutas(empresa: str) -> None:
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT fn_obtener_pedidos_con_pedir_json(%s);", (empresa,))
            raw_ped = cur.fetchone()[0]
    data_ped = json.loads(raw_ped) if isinstance(raw_ped, str) else (raw_ped or [])
    rutas_unicas = sorted(
        {p.get("ruta") for p in data_ped if p.get("ruta") not in (None, "", "null")},
        key=lambda x: str(x),
    )
    total_rutas = len(rutas_unicas)
    print(
        f"[PEDIDOS] empresa={empresa} total_items={len(data_ped)} "
        f"rutas_unicas={total_rutas} lista_rutas={rutas_unicas}",
        flush=True,
    )


def _get_bd() -> str:
    """Obtiene la empresa activa desde la sesión."""

    bd = session.get("empresa")
    if not bd:
        raise ValueError("Falta empresa en sesión")
    return bd


@subir_pedidos_bp.route("/subir-pedidos", methods=["GET"])
@login_required
def subir_pedidos_index():
    """Pantalla principal para consultar y editar placas."""

    ensure_table()
    bd = _get_bd()
    vehiculos = get_vehiculos(bd)
    return render_template("subir_pedidos.html", vehiculos=vehiculos, bd=bd)


@subir_pedidos_bp.route("/vehiculos/placa", methods=["POST"])
@login_required
def guardar_placa():
    """Guarda la placa asociada a una ruta enviada desde la tabla."""

    try:
        data = request.get_json() or {}
        bd = _get_bd()
        ruta = int(data.get("ruta"))
        placa = data.get("placa", "")
        upsert_vehiculo(bd, ruta, placa)
        return jsonify(success=True, data={"ruta": ruta, "placa": placa})
    except Exception as e:
        return jsonify(success=False, error=str(e)), 400


@subir_pedidos_bp.route("/vehiculos/add", methods=["POST"])
@login_required
def agregar_ruta():
    """Crea una nueva ruta vacía y la devuelve al cliente."""

    try:
        bd = _get_bd()
        nuevo = add_ruta(bd)
        return jsonify(success=True, data=nuevo)
    except Exception as e:
        return jsonify(success=False, error=str(e)), 400


@subir_pedidos_bp.route("/vehiculos/delete", methods=["POST"])
@login_required
def eliminar_ruta():
    """Elimina una ruta específica."""

    try:
        data = request.get_json() or {}
        bd = _get_bd()
        ruta = int(data.get("ruta"))
        deleted = delete_ruta(bd, ruta)
        return jsonify(success=deleted, data={"ruta": ruta})
    except Exception as e:
        return jsonify(success=False, error=str(e)), 400


@subir_pedidos_bp.route("/subir-pedidos/login-portal", methods=["POST"])
@login_required
def probar_login_portal():
    """Ejecuta la automatización de login con las credenciales ingresadas."""

    data = request.get_json() or {}
    username = (data.get("usuario") or "").strip()
    password = data.get("contrasena") or ""

    if not username or not password:
        return jsonify(success=False, message="Usuario y contraseña son obligatorios."), 400

    try:
        avances: List[str] = []
        bd = _get_bd()
        try:
            log_pedidos_rutas(bd)
        except Exception as e:
            print(f"WARN [PEDIDOS] {e}", flush=True)

        with iniciar_navegador() as page:
            login_ok = login_portal_grupo_nutresa(
                username=username,
                password=password,
                notificar_estado=avances.append,
                page=page,
            )

            if login_ok:
                archivo = crear_archivo_pedido_masivo("/tmp/pedido_masivo.xlsx")
                avances.append("Login exitoso, iniciando carga de pedidos")
                carga_ok = cargar_pedido_masivo_excel(
                    archivo,
                    notificar_estado=avances.append,
                    page=page,
                )
            else:
                carga_ok = False

        ok = login_ok and carga_ok
        if ok:
            message = "Login y carga de pedidos completados"
        elif login_ok and not carga_ok:
            message = "Login exitoso pero falló la carga de pedidos"
        else:
            message = "Fallo el login: revisa credenciales o selectores"

        return jsonify(success=ok, message=message, avances=avances)
    except Exception as e:
        tb = traceback.format_exc()
        print("ERROR PLAYWRIGHT LOGIN+CARGA\n", tb)
        return jsonify(
            success=False,
            message="Error al ejecutar la automatización",
            error=str(e),
            traceback=tb,
        ), 500
