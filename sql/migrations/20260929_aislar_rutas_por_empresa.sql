-- Aísla la asignación de rutas por BD, cliente y día.
-- Reemplaza el procedimiento; no recarga ni modifica pedidos existentes.
-- Aplicar en la BD de la aplicación según sql/README.md.

CREATE OR REPLACE PROCEDURE etl_cargar_pedidos_y_rutas_masivo(
  p_pedidos JSONB,
  p_rutas   JSONB,
  p_dia     TEXT,
  p_bd      TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM pedxclixprod WHERE bd = p_bd;
  DELETE FROM pedxrutaxprod WHERE bd = p_bd;
  DELETE FROM rutas        WHERE bd = p_bd;
  DELETE FROM materiales   WHERE bd = p_bd;
  DELETE FROM inventario   WHERE bd = p_bd;
  INSERT INTO pedxclixprod (
    bd,
    numero_pedido,
    codigo_cli,
    ruta,
    nombre,
    barrio,
    ciudad,
    asesor,
    codigo_pro,
    producto,
    cantidad,
    valor,
    tip_pro,
    estado,
    codigo_pideky
  )
  SELECT
    p_bd,
    x.numero_pedido,
    split_part(x.cliente, '-', 1),
    NULL,
    x.nombre,
    x.barrio,
    x.ciudad,
    split_part(x.asesor, '-', 1),
    x.codigo_pro,
    x.producto,
    x.cantidad,
    x.valor,
    x.tipo_pro,
    x.estado,
    x.codigo_pideky
  FROM jsonb_to_recordset(p_pedidos) AS x(
    numero_pedido TEXT,
    cliente       TEXT,
    nombre        TEXT,
    barrio        TEXT,
    ciudad        TEXT,
    asesor        TEXT,
    codigo_pro    TEXT,
    producto      TEXT,
    cantidad      INTEGER,
    valor         NUMERIC,
    tipo_pro      TEXT,
    estado        TEXT,
    codigo_pideky TEXT
  )
  WHERE x.tipo_pro   = 'N'
  AND x.estado = 'Sin Descargar' OR x.estado = 'Sin facturar';
  BEGIN
    WITH cte AS (
      SELECT
        p_bd            AS bd,
        (regexp_split_to_array(r.codigo_ruta, '[- ]'))[1]::INTEGER AS codigo_ruta,
        (regexp_split_to_array(r.codigo_ruta, '[- ]'))[2]          AS dia,
        (regexp_split_to_array(r.codigo_cliente, '[- ]'))[1]       AS codigo_cliente
      FROM jsonb_to_recordset(p_rutas) AS r(
        codigo_cliente TEXT,
        codigo_ruta    TEXT
      )
    )
    INSERT INTO rutas (bd, codigo_ruta, dia, codigo_cliente)
    SELECT bd, codigo_ruta, dia, codigo_cliente
    FROM cte
    WHERE dia = p_dia;

  EXCEPTION
    WHEN unique_violation THEN
      RAISE EXCEPTION 'Un cliente no puede estar asignado a dos rutas en el mismo dia';
  END;


  UPDATE pedxclixprod p
  SET ruta = r.codigo_ruta
  FROM rutas r
  WHERE p.bd           = p_bd
    AND r.bd           = p.bd
    AND p.codigo_cli   = r.codigo_cliente
    AND r.dia           = p_dia;
END;
$$;
