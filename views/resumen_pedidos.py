"""Preparación de pedidos y consulta del resumen por cliente."""

RESUMEN_PEDIDOS_SQL = """
    SELECT
        bd,
        codigo_cli,
        MAX(NULLIF(nombre, '')) AS nombre,
        MAX(NULLIF(barrio, '')) AS barrio,
        MAX(NULLIF(ciudad, '')) AS ciudad,
        MAX(NULLIF(asesor, '')) AS asesor,
        MAX(codigo_pideky) AS codigo_pideky,
        COUNT(DISTINCT numero_pedido) AS total_pedidos,
        SUM(valor) AS valor,
        MAX(ruta) AS ruta
    FROM pedxclixprod
    WHERE bd = %s
    GROUP BY bd, codigo_cli
    ORDER BY codigo_cli
"""


def preparar_pedidos(pedidos):
    # Cliente contiene el código usado para consolidar y asignar rutas.
    # Solo se recortan los campos descriptivos, nunca el identificador.
    for pedido in pedidos:
        for campo in ("nombre", "barrio"):
            if isinstance(pedido.get(campo), str):
                pedido[campo] = pedido[campo][:40]
