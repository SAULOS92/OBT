-- Ejecutar con psql -X -v ON_ERROR_STOP=1 -f tests/sql/test_aislamiento_empresas.sql
-- Usa un esquema exclusivo y revierte todos los datos y objetos al terminar.
BEGIN;
CREATE SCHEMA obt_test_aislamiento;
SET LOCAL search_path TO obt_test_aislamiento, pg_catalog;
SET LOCAL plpgsql.check_asserts = on;

CREATE TABLE pedxclixprod (
  bd VARCHAR(25), numero_pedido VARCHAR(70), codigo_cli VARCHAR(30),
  ruta INTEGER, nombre VARCHAR(80), barrio VARCHAR(40), ciudad VARCHAR(30),
  asesor VARCHAR(20), codigo_pro VARCHAR(25), producto VARCHAR(80),
  cantidad INTEGER, valor NUMERIC(14,2), tip_pro VARCHAR(2),
  estado VARCHAR(15), codigo_pideky TEXT
);
CREATE TABLE rutas (
  bd VARCHAR(25), codigo_ruta INTEGER, dia VARCHAR(10), codigo_cliente VARCHAR(30),
  CONSTRAINT rutas_bd_dia_cliente_unique UNIQUE (bd, dia, codigo_cliente)
);
CREATE TABLE pedxrutaxprod (bd VARCHAR(25));
CREATE TABLE materiales (bd VARCHAR(25));
CREATE TABLE inventario (bd VARCHAR(25));

\ir ../../sql/migrations/20260929_aislar_rutas_por_empresa.sql

-- B tiene un pedido del cliente compartido, y una ruta de otro cliente.
-- A no debe modificar ese pedido ni tomar prestada esa ruta.
INSERT INTO pedxclixprod (bd, numero_pedido, codigo_cli, ruta)
VALUES ('empresa_b', 'B-1', '100', 30);
INSERT INTO rutas (bd, codigo_ruta, dia, codigo_cliente)
VALUES ('empresa_b', 20, 'LUN', '200');
INSERT INTO pedxrutaxprod VALUES ('empresa_b');
INSERT INTO materiales VALUES ('empresa_b');
INSERT INTO inventario VALUES ('empresa_b');

CALL etl_cargar_pedidos_y_rutas_masivo(
  '[
    {"numero_pedido":"A-1","cliente":"100","codigo_pro":"P1","cantidad":2,"tipo_pro":"N","estado":"Sin Descargar"},
    {"numero_pedido":"A-1","cliente":"100","codigo_pro":"P2","cantidad":3,"tipo_pro":"N","estado":"Sin Descargar"},
    {"numero_pedido":"A-2","cliente":"200","codigo_pro":"P1","cantidad":4,"tipo_pro":"N","estado":"Sin Descargar"}
  ]',
  '[{"codigo_cliente":"100","codigo_ruta":"10-LUN"}]',
  'LUN', 'empresa_a'
);

DO $$
BEGIN
  ASSERT (SELECT ruta = 30 FROM pedxclixprod WHERE bd = 'empresa_b'),
    'La carga de A modificó un pedido de B con el mismo código de cliente';
  ASSERT (SELECT count(*) = 2 AND bool_and(ruta = 10)
          FROM pedxclixprod WHERE bd = 'empresa_a' AND codigo_cli = '100'),
    'Las líneas del cliente de A no conservaron su propia ruta';
  ASSERT (SELECT ruta IS NULL FROM pedxclixprod
          WHERE bd = 'empresa_a' AND codigo_cli = '200'),
    'Un cliente sin ruta en A heredó la ruta de B';
  ASSERT (SELECT count(*) = 1 FROM pedxrutaxprod WHERE bd = 'empresa_b')
     AND (SELECT count(*) = 1 FROM materiales WHERE bd = 'empresa_b')
     AND (SELECT count(*) = 1 FROM inventario WHERE bd = 'empresa_b')
     AND (SELECT count(*) = 1 FROM rutas WHERE bd = 'empresa_b'),
    'La carga de A eliminó datos de B';
END;
$$;

-- Ambas empresas pueden tener el mismo cliente, en el mismo día y distinta ruta.
CALL etl_cargar_pedidos_y_rutas_masivo(
  '[{"numero_pedido":"B-2","cliente":"100","codigo_pro":"P1","cantidad":9,"tipo_pro":"N","estado":"Sin Descargar"}]',
  '[{"codigo_cliente":"100","codigo_ruta":"30-LUN"}]',
  'LUN', 'empresa_b'
);

DO $$
BEGIN
  ASSERT (SELECT bool_and(ruta = 10) FROM pedxclixprod
          WHERE bd = 'empresa_a' AND codigo_cli = '100'),
    'La carga de B cambió las rutas de A';
  ASSERT (SELECT ruta = 30 FROM pedxclixprod WHERE bd = 'empresa_b'),
    'B recibió la ruta de A para el cliente compartido';
END;
$$;

-- Recargar A cambia solo A, y conserva el filtro por día.
CALL etl_cargar_pedidos_y_rutas_masivo(
  '[
    {"numero_pedido":"A-3","cliente":"100","codigo_pro":"P1","cantidad":5,"tipo_pro":"N","estado":"Sin Descargar"},
    {"numero_pedido":"A-4","cliente":"200","codigo_pro":"P1","cantidad":6,"tipo_pro":"N","estado":"Sin Descargar"}
  ]',
  '[
    {"codigo_cliente":"100","codigo_ruta":"11-LUN"},
    {"codigo_cliente":"200","codigo_ruta":"12-MAR"}
  ]',
  'LUN', 'empresa_a'
);

DO $$
BEGIN
  ASSERT (SELECT ruta = 11 FROM pedxclixprod
          WHERE bd = 'empresa_a' AND codigo_cli = '100'),
    'La recarga de A no actualizó su ruta';
  ASSERT (SELECT ruta IS NULL FROM pedxclixprod
          WHERE bd = 'empresa_a' AND codigo_cli = '200'),
    'Se asignó una ruta de otro día';
  ASSERT (SELECT ruta = 30 AND numero_pedido = 'B-2' AND cantidad = 9
          FROM pedxclixprod WHERE bd = 'empresa_b'),
    'La recarga de A alteró el pedido de B';

  -- Se mantiene el rechazo de clientes duplicados dentro de la misma BD/día.
  BEGIN
    CALL etl_cargar_pedidos_y_rutas_masivo(
      '[]',
      '[{"codigo_cliente":"100","codigo_ruta":"1-LUN"},
        {"codigo_cliente":"100","codigo_ruta":"2-LUN"}]',
      'LUN', 'empresa_a'
    );
    RAISE EXCEPTION 'No se rechazó el cliente duplicado';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM <> 'Un cliente no puede estar asignado a dos rutas en el mismo dia' THEN
      RAISE;
    END IF;
  END;
  ASSERT (SELECT ruta = 11 FROM pedxclixprod
          WHERE bd = 'empresa_a' AND codigo_cli = '100'),
    'La carga rechazada no revirtió sus cambios';
END;
$$;

ROLLBACK;
