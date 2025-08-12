# Documentación Técnica de Endpoints

Este documento describe los endpoints expuestos por la aplicación Flask, junto con las funciones auxiliares y el flujo lógico de cada uno.

## `views/upload.py`

### Funciones auxiliares
- **`_get_json_gzip_aware()`**: Lee el cuerpo de la petición y, si viene comprimido con Gzip, lo descomprime antes de convertirlo a JSON.

### Endpoints
- **`GET /`** y **`GET /cargar-pedidos`** (`upload_index`): muestra el formulario de carga de pedidos.
- **`POST /`** y **`POST /cargar-pedidos`** (`upload_index`):
  1. Conecta a la base de datos y ejecuta el procedimiento almacenado `etl_cargar_pedidos_y_rutas_masivo` con los pedidos y rutas recibidos.
  2. Consulta `fn_obtener_resumen_pedidos` para obtener un resumen en JSON.
  3. Convierte el resumen a Excel en memoria y lo devuelve como descarga.
  4. En caso de error devuelve JSON con el detalle.

## `views/generar_pedidos.py`

### Endpoints
- **`GET /generar-pedidos`** (`generar_pedidos_index`): Renderiza la plantilla principal indicando el negocio en sesión.
- **`POST /generar-pedidos`** (`cargar_pedidos`):
  1. Recibe inventario y materiales en JSON.
  2. Si llegan materiales y el negocio no es `nutresa`, ejecuta `sp_cargar_materiales` y valida que no queden sin definir.
  3. Ejecuta `sp_etl_pedxrutaxprod_json` para procesar el inventario.
  4. Construye un archivo ZIP en memoria mediante `_build_zip` y lo envía como descarga.

### Funciones auxiliares
- **`_build_zip(empresa)`**: consulta las funciones `fn_obtener_reparticion_inventario_json` y `fn_obtener_pedidos_con_pedir_json`, genera hojas de Excel y arma un ZIP en memoria.

## `views/consolidar_compras.py`

### Endpoints
- **`GET /consolidar-compras`** (`consolidar_compras_index`): muestra el formulario para subir reportes.
- **`POST /consolidar-compras`** (`consolidar_compras_index`):
  1. Lee un Excel cargado por el usuario.
  2. Valida y agrupa columnas según `COLUMN_CONFIG`.
  3. Genera un Excel consolidado para `celluweb` o `ecom` y opcionalmente un CSV adicional.
  4. Guarda los archivos en un directorio temporal para su descarga posterior.
- **`GET /consolidar-compras/download/<filename>`** (`descargar_archivo_file`): envía el archivo previamente generado.

## `views/auth.py`

### Funciones
- **`login_required`**: decorador que exige que `user_id` esté presente en la sesión.
- **`login`** (`GET`/`POST /login`): valida credenciales contra la base de datos y establece información de sesión.
- **`logout`** (`GET /logout`): limpia la sesión y redirige al formulario de login.

## `views/auditoria.py`

### Endpoints
- **`GET /auditoria`** (`auditoria_view`): muestra el formulario de auditoría.
- **`POST /auditoria/descargar`** (`descargar_excel`):
  1. Consulta las tablas `PEDXCLIXPROD` y `pedxrutaxprod` filtradas por empresa.
  2. Exporta ambas a un Excel con dos hojas y lo envía como descarga.

## `views/subir_pedidos.py`

### Funciones auxiliares de base de datos
- **`_ensure_table()`**: crea la tabla `vehiculos` si no existe.
- **`_get_bd()`**: obtiene la empresa actual desde la sesión.
- **`_get_vehiculos(bd)`**: devuelve las rutas/placas registradas y su estado.
- **`_upsert_vehiculo(bd, ruta, placa)`**: inserta o actualiza una placa.
- **`_add_ruta(bd)`**: crea un nuevo registro de ruta con placa vacía.

### Funciones de Selenium y jobs
- **`subir_pedidos_ruta(bd, ruta, usuario, password)`**: genera un Excel con pedidos de la ruta, inicia un navegador headless y carga los pedidos en el portal.
- **`_job_runner(...)`** y **`_enqueue_job(...)`**: gestionan la ejecución asíncrona de `subir_pedidos_ruta` y el seguimiento de estados.

### Endpoints
- **`GET /subir-pedidos`** (`subir_pedidos_index`): asegura la existencia de la tabla y renderiza la pantalla con las rutas actuales.
- **`POST /vehiculos/placa`** (`guardar_placa`): guarda o actualiza la placa asociada a una ruta.
- **`POST /vehiculos/add`** (`agregar_ruta`): crea una nueva ruta vacía.
- **`POST /vehiculos/play`** (`ejecutar_ruta`): encola la ejecución de `subir_pedidos_ruta` para la ruta indicada.
- **`GET /vehiculos/estado`** (`estado_ruta`): devuelve el estado de ejecución del job de una ruta.

## `db.py`

- **`conectar()`**: abre una conexión PostgreSQL usando la variable `DATABASE_URL` y `sslmode=require`.

