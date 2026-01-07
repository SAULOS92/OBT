"""Rutas HTTP para gestionar la vista de subir pedidos."""

import json
import traceback
from typing import Any, Dict, List

from flask import jsonify, render_template, request, session
from openpyxl import Workbook

from db import conectar
from views.auth import login_required

from . import subir_pedidos_bp
from .automation import cargar_pedido_masivo_excel, iniciar_navegador, login_portal_grupo_nutresa
from .vehiculos import (
    PLACA_MAX_LEN,
    add_ruta,
    delete_ruta,
    ensure_table,
    get_vehiculos,
    upsert_vehiculo,
)


def log_pedidos_rutas(empresa: str) -> Dict[str, Any]:
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT fn_obtener_pedidos_con_pedir_json(%s);", (empresa,))
            raw_ped = cur.fetchone()[0]

    data_ped = json.loads(raw_ped) if isinstance(raw_ped, str) else (raw_ped or [])
    rutas_unicas = sorted(
        {p.get("ruta") for p in data_ped if p.get("ruta") not in (None, "", "null")},
        key=lambda x: str(x),
    )

    # Traer placas desde tabla vehiculos
    ensure_table()
    vehiculos = get_vehiculos(empresa)

    placas_por_ruta = {}
    for v in vehiculos or []:
        ruta_v = None
        placa_v = ""

        if isinstance(v, dict):
            ruta_v = v.get("ruta")
            placa_v = (v.get("placa") or "").strip()
        elif hasattr(v, "ruta"):
            ruta_v = getattr(v, "ruta", None)
            placa_v = (getattr(v, "placa", "") or "").strip()
        elif isinstance(v, (list, tuple)) and len(v) >= 2:
            ruta_v = v[0]
            placa_v = (v[1] or "").strip()

        if ruta_v is None:
            continue

        ruta_key = str(ruta_v).strip()
        if placa_v:
            placas_por_ruta[ruta_key] = placa_v

    rutas_con_placa = []
    for r in rutas_unicas:
        r_key = str(r).strip()
        placa = (placas_por_ruta.get(r_key) or r_key)
        rutas_con_placa.append({"ruta": r, "placa": placa})

    print(
        f"[PEDIDOS] empresa={empresa} total_rutas={len(rutas_unicas)} "
        f"rutas={json.dumps(rutas_con_placa, ensure_ascii=False)}",
        flush=True,
    )

    return {
        "rutas_con_placa": rutas_con_placa,
        "data_ped": data_ped,
        "total_rutas": len(rutas_unicas),
    }


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
    return render_template(
        "subir_pedidos.html", vehiculos=vehiculos, bd=bd, placa_max_len=PLACA_MAX_LEN
    )


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
    campo_placa = (data.get("campo_placa") or "observaciones").strip().lower()

    if campo_placa not in {"observaciones", "purchase_order"}:
        campo_placa = "observaciones"

    if not username or not password:
        return jsonify(success=False, message="Usuario y contraseña son obligatorios."), 400

    try:
        avances: List[str] = []
        bd = _get_bd()
        try:
            info = log_pedidos_rutas(bd)
        except Exception as e:
            print(f"WARN [PEDIDOS] {e}", flush=True)
            info = {"rutas_con_placa": [], "data_ped": [], "total_rutas": 0}

        rutas_con_placa = info.get("rutas_con_placa") or []
        data_ped = info.get("data_ped") or []
        total_rutas = info.get("total_rutas")

        if not rutas_con_placa:
            return (
                jsonify(success=False, message="No hay rutas/pedidos para procesar"),
                400,
            )

        hay_pedidos_en_rutas = any(
            str(p.get("ruta")) == str(r.get("ruta"))
            for r in rutas_con_placa
            for p in data_ped
        )

        if not hay_pedidos_en_rutas:
            return jsonify(success=False, message="No hay pedidos para procesar"), 400

        carga_ok = True
        error_en_proceso = False
        ruta_fallo = None
        placa_fallo = None

        with iniciar_navegador() as page:
            login_ok = login_portal_grupo_nutresa(
                username=username,
                password=password,
                notificar_estado=avances.append,
                page=page,
            )

            if login_ok:
                archivo = "/tmp/pedido_masivo.xlsx"
                avances.append("Login exitoso, iniciando carga de pedidos")

                for ruta_placa in rutas_con_placa:
                    try:
                        avances.append(
                            f"Procesando ruta: {ruta_placa.get('ruta')} placa={ruta_placa.get('placa')}"
                        )

                        pedidos_ruta = [
                            p
                            for p in data_ped
                            if str(p.get("ruta")) == str(ruta_placa.get("ruta"))
                        ]

                        if not pedidos_ruta:
                            avances.append(
                                f"Ruta {ruta_placa.get('ruta')}: sin pedidos, se omite"
                            )
                            continue

                        wb = Workbook()
                        ws = wb.active

                        ws.append([])
                        ws.append([])
                        ws.append([])
                        ws.append(["codigo_pro", "producto", "UN", "pedir"])

                        for pedido in pedidos_ruta:
                            ws.append(
                                [
                                    pedido.get("codigo_pro"),
                                    pedido.get("producto"),
                                    "UN",
                                    pedido.get("pedir"),
                                ]
                            )

                        wb.save(archivo)
                    except Exception:
                        tb = traceback.format_exc()
                        print(f"ERROR al crear Excel de pedidos\n{tb}", flush=True)
                        avances.append(
                            f"Ruta {ruta_placa.get('ruta')} placa={ruta_placa.get('placa')}: falló la carga"
                        )
                        error_en_proceso = True
                        ruta_fallo = ruta_placa.get("ruta")
                        placa_fallo = ruta_placa.get("placa")
                        try:
                            page.close()
                        except Exception:
                            pass
                        carga_ok = False
                        break

                    try:
                        ruta_cargada = cargar_pedido_masivo_excel(
                            ruta_placa,
                            archivo,
                            campo_placa,
                            notificar_estado=avances.append,
                            page=page,
                        )
                        if ruta_cargada:
                            avances.append(
                                f"Ruta {ruta_placa.get('ruta')}: carga OK"
                            )
                        else:
                            avances.append(
                                f"Ruta {ruta_placa.get('ruta')} placa={ruta_placa.get('placa')}: falló la carga"
                            )
                            error_en_proceso = True
                            ruta_fallo = ruta_placa.get("ruta")
                            placa_fallo = ruta_placa.get("placa")
                            carga_ok = False
                            break
                    except Exception:
                        tb = traceback.format_exc()
                        print(f"ERROR al cargar pedidos\n{tb}", flush=True)
                        avances.append(
                            f"Ruta {ruta_placa.get('ruta')} placa={ruta_placa.get('placa')}: falló la carga ({tb.strip()})"
                        )
                        error_en_proceso = True
                        ruta_fallo = ruta_placa.get("ruta")
                        placa_fallo = ruta_placa.get("placa")
                        try:
                            page.close()
                        except Exception:
                            pass
                        carga_ok = False
                        break

            else:
                carga_ok = False

        if error_en_proceso:
            return (
                jsonify(
                    success=False,
                    message=(
                        f"Falló la carga en la ruta {ruta_fallo}; proceso abortado"
                    ),
                    avances=avances,
                ),
                500,
            )

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
