# Aislamiento de pedidos por empresa

La asignación de rutas de `etl_cargar_pedidos_y_rutas_masivo` debe identificar al
cliente por `(bd, codigo_cli)` y buscar su ruta en la misma `bd` y el día indicado.
El código de cliente puede repetirse entre empresas.

## Aplicación del cambio

`migrations/20260929_aislar_rutas_por_empresa.sql` reemplaza únicamente ese
procedimiento. Su definición coincide con la de `MEMPRY FACT`, incluyendo el
filtro que limita tanto los pedidos modificados como las rutas utilizadas a la
empresa recibida en `p_bd`.

Antes de aplicarlo, comparar la definición desplegada con la versión del
repositorio para conservar cualquier cambio que exista solo en la BD:

```sql
SELECT pg_get_functiondef(
  'public.etl_cargar_pedidos_y_rutas_masivo(jsonb,jsonb,text,text)'::regprocedure
);
```

Aplicar con una conexión autorizada a la BD de la aplicación, cuyo esquema debe
estar en el `search_path`:

```sh
psql -X "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/migrations/20260929_aislar_rutas_por_empresa.sql
```

Desplegar Python no actualiza automáticamente el procedimiento de PostgreSQL.
La migración tampoco repara rutas previamente asignadas de forma incorrecta:
después de aplicarla se deben recargar los pedidos y las rutas de las empresas
afectadas desde sus archivos de origen y volver a generar sus consolidados.

## Regresiones

En una base de pruebas PostgreSQL, con permiso para crear esquemas:

```sh
psql -X "$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f tests/sql/test_aislamiento_empresas.sql
python -m unittest discover -s tests -v
```

La prueba SQL crea un esquema exclusivo dentro de una transacción y termina con
`ROLLBACK`. Comprueba clientes compartidos, clientes sin ruta propia, recargas,
selección de día, conservación de datos ajenos y rechazo de duplicados dentro de
la misma empresa. Las pruebas Python usan sesiones Flask y Excel reales, con
la BD y el portal sustituidos, para comprobar cargas simultáneas y limpieza de
temporales cuando falla la subida.

## Alcance de la revisión

Los filtros de resúmenes, inventario, auditoría y vehículos revisados incluyen
la empresa. Además del SQL, las cargas al portal ahora usan un directorio
temporal distinto por petición y lo eliminan al finalizar.

La identificación de la empresa en `views/auth.py` sigue derivándose del primer
segmento del dominio del correo. Si dos empresas distintas comparten ese
segmento (por ejemplo, `empresa.com` y `empresa.net`), reciben la misma `bd`.
Resolver ese caso requiere una asignación explícita de usuarios a empresas y
una migración de las identificaciones existentes; no forma parte de esta
corrección de clientes repetidos entre valores de `bd` distintos.
