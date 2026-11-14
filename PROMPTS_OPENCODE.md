# Registro de prompts y decisiones

## Etapa 1 — diagnóstico y planificación

**Fecha:** 2026-11-14  
**Modelo utilizado:** GPT-6 Luna (OpenAI), según la configuración de la sesión.

### Prompt recibido

> Vamos a desarrollar una investigación científica reproducible para la asignatura Sistemas Distribuidos y Paradigmas de la Computación de Ingeniería en Telemática, ESPOCH.
>
> Autor: Alan David Monar Benalcazar.
>
> Tema: Benchmark de Throughput y Retención de Mensajes en Arquitecturas de Streaming: Apache Kafka vs. Cloud Pub/Sub Lite ante Cargas Masivas de Eventos.
>
> El proyecto debe contener exactamente estos ocho archivos:
> - articulo.pdf
> - articulo.qmd
> - references.bib
> - _quarto.yml
> - benchmark.py
> - results.csv
> - README.md
> - PROMPTS_OPENCODE.md
>
> El artículo debe tener entre 3 y 4 páginas en formato IEEE Conference, con 5 a 8 referencias bibliográficas científicas verificables.
>
> Trabajaremos por etapas para ahorrar recursos y asegurar el rigor experimental.
>
> ETAPA 1: Diagnóstico y planificación.
> 1. Inspecciona el entorno Ubuntu y verifica la disponibilidad de Python, Java, Docker, Apache Kafka, Git, Quarto y LaTeX.
> 2. Investiga la viabilidad actual de utilizar Cloud Pub/Sub Lite, considerando su retirada anunciada por Google.
> 3. Propón una metodología experimental reproducible para medir throughput, latencia, pérdida y retención de mensajes.
> 4. Considera cargas de 10.000, 100.000 y 1.000.000 de eventos de 1 KB, con tres repeticiones por escenario. Ajusta las cargas si existen limitaciones justificadas.
> 5. Si Pub/Sub Lite no está disponible, plantea una simulación fundamentada en documentación oficial, diferenciándola estrictamente de las mediciones reales.
> 6. Crea PROMPTS_OPENCODE.md para registrar este prompt, el modelo utilizado y las decisiones metodológicas.
>
> Restricciones:
> - No inventes resultados ni referencias bibliográficas.
> - No instales dependencias ni ejecutes pruebas masivas sin mi autorización.
> - No publiques todavía el repositorio.
> - No modifiques archivos fuera del directorio actual.
> - Mantén las respuestas concisas para ahorrar tokens.
>
> Entrega únicamente el diagnóstico del entorno, la propuesta metodológica y el plan de implementación. Espera mi aprobación antes de avanzar a la siguiente etapa.

### Diagnóstico observado

Ubuntu 26.04.1 LTS. `python3` está disponible (3.14.4), pero el módulo `pip` no está instalado. `git` está disponible (2.53.0). No se encontraron `java`, `docker`, `kafka`, `quarto`, `pdflatex` ni `latexmk` en el PATH. El directorio de trabajo estaba vacío al inicio. No se instalaron dependencias ni se ejecutaron pruebas.

### Decisiones metodológicas propuestas (pendientes de aprobación)

1. **Comparación justa:** registrar versiones, topología, particiones, replicación, configuración de retención, recursos de CPU/memoria/disco/red y configuración del productor/consumidor. Igualar las garantías de entrega (confirmación/acks) y documentar las diferencias inevitables entre servicios.
2. **Carga:** eventos con cuerpo de exactamente 1.024 bytes, incluyendo ID único y marca temporal de emisión, completado con relleno determinista; metadatos de transporte fuera del cuerpo cuando la API lo permita. Ejecutar 10.000, 100.000 y 1.000.000 de eventos, tres repeticiones por escenario. Realizar primero una corrida pequeña de validación; pausar ante errores, límites, costes inesperados o recursos insuficientes. No reducir cargas ni iniciar experimentos hasta autorización.
3. **Métricas:** throughput de publicación confirmada (eventos/s y MiB/s); latencia extremo a extremo por evento con reloj monotónico común en un mismo host, reportando p50/p95/p99; pérdida como IDs únicos no recibidos respecto de los confirmados/enviados; duplicados y errores por separado. Guardar resultados crudos y configuración por corrida.
4. **Retención:** configurar/documentar el periodo disponible en cada sistema; confirmar persistencia después de cerrar productor y consumidor y leer luego de una demora controlada. Repetir a varios puntos de edad factibles, registrando antigüedad más antigua recuperable. No extrapolar una corrida corta a una garantía de retención prolongada.
5. **Reproducibilidad:** script parametrizable, semilla fija para el relleno, reloj y sincronización documentados, limpieza entre corridas, tres repeticiones independientes y CSV con una fila por corrida. Registrar fallos como tales; no fabricar datos ni completar resultados faltantes.
6. **Pub/Sub Lite:** la documentación oficial de Google indica que el servicio está deprecated y se apagará el **31 de enero de 2027**; dejó de estar disponible para nuevos clientes después del **24 de septiembre de 2024**. Además, clientes sin uso durante el periodo 15-abr-2025 a 15-jul-2025 perdieron acceso desde el 15-jul-2025. Por tanto, no es una opción viable para un proyecto nuevo salvo que el autor ya tenga un proyecto/cuenta con acceso activo. Fuente oficial: <https://cloud.google.com/pubsub/lite/docs>.
7. **Alternativa no equivalente:** si no existe acceso autorizado, no presentar simulación como benchmark de Pub/Sub Lite ni mezclar datos simulados con mediciones. Puede incluirse un modelo analítico/simulación etiquetado claramente, basado en parámetros explícitos extraídos de documentación oficial y con análisis de sensibilidad; sus resultados no son mediciones de rendimiento del servicio.

### Plan de implementación tras aprobación

1. Confirmar acceso/cuenta de Pub/Sub Lite, presupuesto y autorización para instalaciones y pruebas.
2. Fijar topología y parámetros comparables; elegir entre comparación real con Pub/Sub Lite, o Kafka real más análisis/simulación claramente separado.
3. Implementar `benchmark.py` y el esquema de `results.csv`; validar primero con carga mínima autorizada.
4. Ejecutar las cargas y repeticiones solo con aprobación explícita; conservar resultados y registrar incidencias.
5. Redactar `articulo.qmd`, `references.bib`, `_quarto.yml` y `README.md`; verificar referencias científicas y compilar PDF IEEE de 3–4 páginas cuando Quarto/LaTeX estén disponibles/autorizados.
6. Revisar que el directorio final contenga exactamente los ocho archivos solicitados. No publicar el repositorio sin autorización.

## Etapa 2 — implementación y piloto

**Fecha:** 2026-11-14  
**Modelo utilizado:** GPT-6 Luna (OpenAI), según la configuración de la sesión.

### Prompt recibido

> Apruebo la etapa 1. Vamos a continuar con la ETAPA 2: implementación del entorno experimental y prueba piloto.
>
> DECISIONES APROBADAS:
> - Apache Kafka se ejecutará localmente en Ubuntu mediante Java y KRaft, sin Docker.
> - Cloud Pub/Sub Lite se evaluará mediante un modelo analítico basado en documentación oficial, debido a la restricción de acceso para nuevos clientes. No presentar resultados analíticos como mediciones reales.
> - No utilizar servicios de Google Cloud que generen costes.
> - Mantener el repositorio independiente de otros proyectos.
>
> TAREAS:
> 1. Verifica el espacio disponible en disco, memoria RAM, procesador y arquitectura del sistema.
> 2. Prepara la instalación de Java 21, pip y un entorno virtual de Python. Comprueba la compatibilidad de confluent-kafka con Python 3.14.4. Si existe algún problema, propone una versión compatible de Python sin modificar la instalación principal.
> 3. Descarga una versión estable de Apache Kafka desde su sitio oficial y verifica su integridad. Configúrala en modo KRaft, con un único broker, una partición y factor de replicación 1.
> 4. Instala o prepara Quarto y TinyTeX, verificando que permitan generar un artículo IEEE Conference a doble columna.
> 5. Desarrolla benchmark.py con argumentos configurables para cantidad de eventos, tamaño, repeticiones y servidor Kafka.
> 6. Implementa eventos de exactamente 1024 bytes con identificadores únicos, contenido determinista y compresión desactivada.
> 7. Utiliza publicación asíncrona, confirmaciones acks=all, control de errores y medición mediante time.perf_counter_ns().
> 8. Mide por separado:
>    - Throughput de publicación confirmado, en mensajes/s y MiB/s.
>    - Throughput de consumo.
>    - Latencia de confirmación p50, p95 y p99.
>    - Eventos únicos recuperados.
>    - Eventos faltantes, duplicados y errores.
>    - Retención mediante una segunda lectura con un grupo consumidor nuevo tras una demora controlada.
> 9. Implementa el modelo analítico de Pub/Sub Lite utilizando límites de publicación, particiones y almacenamiento documentados oficialmente. Especifica las ecuaciones, unidades, supuestos y fuentes. No inventes latencias ni tasas de pérdida.
> 10. Diseña results.csv con una columna que identifique claramente el origen de cada resultado: medición real o estimación analítica. Registra configuración, repetición y estado de cada prueba.
> 11. Ejecuta únicamente una prueba piloto de 1000 eventos para verificar que el benchmark funciona. No ejecutes todavía las cargas de 10000, 100000 y 1000000.
> 12. Actualiza PROMPTS_OPENCODE.md con este prompt, decisiones, comandos ejecutados, errores y correcciones reales.
>
> RESTRICCIONES:
> - Autorizo instalar las dependencias necesarias para este proyecto. Si algún comando requiere sudo o contraseña, muéstrame el comando para que lo ejecute personalmente.
> - Utiliza un directorio específico para las herramientas, el entorno virtual y los datos temporales de Kafka. No modifiques otros proyectos ni elimines datos existentes.
> - No incluyas dependencias instaladas ni archivos temporales en GitHub. El repositorio final debe contener los ocho archivos exigidos.
> - No inventes resultados.
> - No publiques todavía el repositorio.
> - No ejecutes pruebas masivas.
> - Mantén las respuestas concisas para ahorrar tokens.
>
> ENTREGABLE DE ESTA ETAPA:
> Presenta las versiones instaladas, configuración del broker, resultado de la prueba piloto, métricas obtenidas, archivos creados y cualquier problema pendiente.
>
> Detente al finalizar la prueba piloto para que pueda revisar los resultados.

### Recursos y compatibilidad verificados

- `df -h .`: 144 GiB de disco, 98 GiB libres al inicio. RAM: 14 GiB (8.6 GiB disponibles). CPU: Intel Core i7-1165G7, 4 núcleos/8 hilos. Arquitectura x86_64.
- Java instalado localmente en `tools/java`: Eclipse Temurin 21.0.13+9; SHA-256 del archivo descargado verificado.
- Python del sistema sigue intacto: 3.14.4. Se preparó `.venv` y `pip` 26.3 dentro del entorno virtual. `confluent-kafka` 2.16.1 se instaló correctamente desde wheel CPython 3.14/Linux x86_64; PyPI publica wheel compatible cp314 y declara Python >=3.8. No fue necesario instalar otra versión de Python.
- Apache Kafka 4.4.0 (Scala 2.13), tarball de `downloads.apache.org`; SHA-512 oficial comparado y coincide. Kafka usa el JDK local con `JAVA_HOME`/`PATH`; no Docker, no sudo.
- Quarto 1.11.9 instalado bajo `tools/quarto`; TinyTeX/TeX Live 2026 bajo `tools/tinytex/current`. Instalados paquetes `ieeetran`, `latexmk`, `xetex`, `collection-latexrecommended` y `collection-fontsrecommended`. Un documento de prueba se compiló en PDF con `documentclass: IEEEtran`, `classoption: conference` y pdfTeX. PDF de validación: tamaño Letter, una página.
- El `quarto check` señaló R y Jupyter ausentes; no son necesarios para compilar el QMD/PDF de este artículo. No instalé R ni Jupyter.

### Configuración y decisiones implementadas

- Broker KRaft combinado broker/controlador, `node.id=1`, direcciones locales 127.0.0.1:9092/9093, almacenamiento en `data/kafka/logs`, `num.partitions=1`, replicación por defecto y `min.insync.replicas` en 1. Cluster ID local almacenado en `data/kafka/cluster-id`.
- `benchmark.py` ofrece modo Kafka y `pubsublite-analytical`; parámetros CLI para eventos, bytes, repeticiones, servidor, espera de retención, timeout, particiones y almacenamiento. Kafka crea un topic único por corrida (no borra topics ni mensajes).
- Cuerpo de evento: 8 bytes de secuencia + 8 bytes de reloj monotónico + relleno determinista `0xA5`, exactamente 1.024 bytes. Key distinta por ID; `acks=all`, idempotencia habilitada y `compression.type=none`.
- Productor asíncrono de `confluent-kafka`; latencia desde `time.perf_counter_ns()` previo a `produce` hasta callback de confirmación. Throughput confirmado mide desde primer envío hasta `flush` completo. Consumidor nuevo mide recuperación y throughput; otro grupo nuevo relee tras 5 s.
- Modelo de Pub/Sub Lite: escenario zonal/no-reservation, una partición, capacidad configurada explícitamente en el mínimo documentado de 4 MiB/s para publicación y suscripción, almacenamiento de 30 GiB por partición (mínimo configurable). Límite por zona de 64 MiB/s para publicación y 128 MiB/s para suscripción. Para el modelo:
  - tamaño almacenado supuesto = `1.024 + 12 = 1.036 bytes/evento` (sin atributos ni ordering key; timestamp de servicio según documentación).
  - tasa límite calculada = `min(particiones × capacidad por partición, cuota por zona) × 2^20 / 1.036`, en eventos/s.
  - capacidad de mensajes = `particiones × almacenamiento GiB × 2^30 / 1.036`, en eventos.
  - Para los parámetros seleccionados, el límite configurado es 4 MiB/s = 4.048,555985 eventos/s y 30 GiB ≈ 31.092.909 eventos almacenados. Son límites/capacidades analíticas, no rendimiento observado; latencia y pérdida quedan vacías/no modeladas.
- Fuentes oficiales del modelo: <https://cloud.google.com/pubsub/lite/docs/topics>, <https://cloud.google.com/pubsub/lite/docs/reservations> y <https://cloud.google.com/pubsub/lite/quotas>.

### Comandos principales ejecutados

- `df -h .`, `free -h`, `lscpu`, `uname -m`.
- Descarga y verificación de Temurin 21, Kafka 4.4.0 (SHA-512 oficial), Quarto 1.11.9, TinyTeX 2026.11 y `get-pip.py`.
- `.venv/bin/python tools/bootstrap/get-pip.py`; `.venv/bin/python -m pip install confluent-kafka==2.16.1`.
- `kafka-storage.sh random-uuid`, `kafka-storage.sh format --no-initial-controllers ...`, `kafka-storage.sh info ...`; luego `kafka-server-start.sh data/kafka/server.properties`.
- `quarto check` y `quarto render data/ieee-check.qmd --to pdf`.
- `.venv/bin/python benchmark.py --mode pubsublite-analytical --events 10000 100000 1000000 --partitions 1 --storage-gib 30 --model-publish-mib-per-s 4 --model-subscribe-mib-per-s 4 --output results.csv`.
- Piloto autorizado: `.venv/bin/python benchmark.py --mode kafka --events 1000 --size-bytes 1024 --repetitions 1 --bootstrap-servers 127.0.0.1:9092 --retention-delay-seconds 5 --timeout-s 60 --output results.csv`.

### Errores y correcciones observados

- `python3 -m ensurepip` falló porque Ubuntu no incluye `ensurepip`; se instaló pip dentro de `.venv` usando `get-pip.py`, sin alterar Python del sistema.
- La primera invocación de Kafka no encontró `java`; se corrigió configurando `JAVA_HOME` y `PATH` al JDK del proyecto.
- Kafka 4.4 no acepta `--standalone` junto con `controller.quorum.voters`; se creó el directorio de logs y se formateó con `--no-initial-controllers`.
- TinyTeX: primer uso de tlmgr con ruta relativa no encontró su raíz; se corrigieron ruta absoluta y `TEXMFCNF`. Se actualizó tlmgr. El nombre del paquete es `ieeetran` en minúsculas; se instaló con ese nombre.
- Primer intento del piloto tuvo un bug (`range()` recibió la lista de eventos) y no produjo métricas; corregido usando el conteo escalar. Dejó un topic vacío `bench-687380b0380c4e6c9934`, preservado deliberadamente. El piloto corregido sí completó. En la revisión editorial se corrigió la interpretación del registro histórico: el texto disponible es `Coordinator load in progress` y no permite adjudicar específicamente ese reintento a la adquisición del PID.

### Resultado del piloto Kafka

Una corrida de 1.000 mensajes, 1.024 bytes, un topic/partición, factor 1, acks all, sin compresión:

| Métrica | Resultado |
|---|---:|
| Estado | `ok` |
| Publicación confirmada | 956,403 mensajes/s; 0,933987 MiB/s |
| Consumo | 316,041 mensajes/s; 0,308633 MiB/s |
| Latencia de confirmación p50 / p95 / p99 | 1.041,547 / 1.043,384 / 1.043,603 ms |
| IDs únicos recuperados | 1.000 |
| Faltantes / duplicados / errores | 0 / 0 / 0 |
| Segunda lectura tras 5 s | 1.000 recuperados; 0 faltantes |

Resultados analíticos y medición real están diferenciados en la columna `origin` de `results.csv`. No se ejecutaron las cargas de 10.000, 100.000 ni 1.000.000 en Kafka. No se eliminaron datos, no se usó Google Cloud ni se publicó el repositorio.

## Etapa 3 — auditoría metodológica y benchmark definitivo

**Fecha:** 2026-11-14  
**Modelo utilizado:** GPT-6 Luna (OpenAI), según la configuración de la sesión.

### Prompt recibido

> Apruebo la etapa 2. Continuemos con la ETAPA 3: auditoría metodológica y benchmark experimental completo.
>
> Conserva todos los resultados del piloto y del intento fallido. No sobrescribas ni ocultes evidencia anterior.
>
> FASE A: AUDITORÍA DEL BENCHMARK
> 1. Revisa benchmark.py completo, especialmente la medición de tiempos, confirmaciones y consumo.
> 2. Investiga por qué el piloto obtuvo una latencia p50 de 1041,547 ms y percentiles muy similares. Comprueba si intervienen la inicialización del productor, la obtención de metadatos, linger.ms, poll(), flush() o tiempos de espera.
> 3. Verifica que los callbacks de confirmación se atiendan periódicamente durante la publicación asíncrona.
> 4. Separa el tiempo de inicialización del productor y consumidor del tiempo efectivo de transferencia. Registra ambos cuando sean relevantes.
> 5. Realiza un calentamiento previo fuera del intervalo medido y utiliza un topic independiente para evitar contaminar las pruebas.
> 6. Comprueba que los tiempos del consumidor no estén dominados por la espera de asignación inicial de particiones.
> 7. Diferencia la latencia de confirmación observada por la aplicación de la latencia interna del broker. No presentes ambas como equivalentes.
> 8. Verifica el tratamiento de errores, identificadores únicos, duplicados, payloads, timeouts y confirmaciones pendientes.
> 9. Mantén acks=all, compresión desactivada, una partición y factor de replicación 1. Documenta también linger.ms, batch.size, idempotencia y demás parámetros relevantes.
> 10. Configura explícitamente la política de retención de Kafka y documenta retention.ms, retention.bytes, cleanup.policy y la configuración de segmentos.
> 11. Verifica las ecuaciones de Pub/Sub Lite con documentación oficial: una partición zonal, 4 MiB/s de publicación, 30 GiB de almacenamiento y 1036 bytes contabilizados por mensaje. Distingue throughput de payload del throughput contabilizado por el servicio.
> 12. Registra cualquier cambio metodológico y su justificación en PROMPTS_OPENCODE.md.
>
> FASE B: VALIDACIÓN
> Ejecuta un nuevo piloto de 1000 eventos utilizando el benchmark auditado. Comprueba 1000 publicaciones confirmadas, 1000 identificadores únicos recuperados, cero faltantes y duplicados, cero errores, métricas temporales coherentes y positivas y recuperación mediante un grupo consumidor nuevo. No establezcas artificialmente un throughput/latencia esperado.
>
> FASE C: BENCHMARK DEFINITIVO
> Si el piloto auditado supera las verificaciones, autorizo ejecutar 10000, 100000 y 1000000 eventos de 1024 bytes, tres repeticiones cada uno. Utiliza topics independientes e identificadores de ejecución únicos; configuración constante; mide throughput de publicación y consumo, p50/p95/p99, duración, confirmados/recuperados/faltantes/duplicados/errores y relectura tras al menos 5 s. Añade comprobación de retención a 30 o 60 s y registra tiempo desde publicación y consumo inicial. No confundas segundos con validación de retención multidiaria. Registra fecha, configuración, ID, versión y estado. Controla memoria, disco y timeouts; detén ante fallas.
>
> FASE D: ANÁLISIS ESTADÍSTICO
> Actualiza results.csv con mediciones, pilotos, fallos y estimaciones identificadas. Calcula media, desviación estándar y coeficiente de variación del throughput usando solo las tres repeticiones definitivas válidas; no mezcles poblaciones ni promedies percentiles como si fueran globales. Calcula recuperación/pérdida. Separa modelo Pub/Sub Lite de Kafka; no compares un límite teórico con rendimiento real. Explica limitaciones.
>
> RESTRICCIONES: no inventar, ajustar ni eliminar resultados; no usar servicios de pago; no modificar fuera del proyecto; no publicar en GitHub; no redactar aún el artículo; conservar trazabilidad; respuestas concisas.
>
> ENTREGA: auditoría, investigación de la latencia, nuevo piloto, tabla de nueve pruebas, estadísticas por escenario, estimación analítica y limitaciones. Detenerse tras esta etapa.

### Auditoría y cambio metodológico

- El CSV de Etapa 2 se copió byte por byte a `data/results_stage2_preserved.csv` antes de ampliar su esquema. Se preservaron todas sus filas y valores en `results.csv`; el intento fallido y el piloto anterior no se borraron. También se guardó una instantánea pre-resumen en `data/results_before_stage3_summary.csv`.
- Hallazgo del piloto original (v0.2): el cronómetro empezaba antes del primer `produce()`, sin precarga explícita de metadatos ni warm-up. `Producer()` se construía antes del timer, por lo que el constructor no explica esos ~1.04 s; el primer envío podía coincidir con inicialización y obtención de metadatos. El registro disponible consigna `Coordinator load in progress`; no se conserva evidencia suficiente para adjudicar específicamente el reintento a la adquisición de PID. `linger.ms=0`, por tanto no atribuyo la espera a linger. `poll(0)` se llamaba en cada iteración y `flush()` al final; ambos pueden despachar callbacks. La API confirma que callbacks se ejecutan al llamar `poll()`/`flush()`. No existe traza histórica por callback que permita determinar qué llamada despachó cada ack; cualquier explicación de la mediana casi común (~1.04 s) como espera de inicialización y primera tanda es una hipótesis, no causalidad demostrada. Se conserva sin alterar la cifra histórica.
- El cliente auditado (v0.3.1-audit) registra constructor, metadatos, warm-up, asignación de consumidor y primer mensaje aparte. El productor emite un warm-up confirmado en un topic diferente antes de iniciar el timer de publicación. Hace `poll(0)` por mensaje; callbacks se atienden durante el envío, no se difieren intencionadamente hasta `flush()`. `flush()` solo cierra el intervalo cuando ya no quedan confirmaciones.
- En el piloto auditado, productor: constructor 0,214 ms; metadatos 53,879 ms; warm-up/ack 1,721/1,710 ms; p50 de ack observada por la aplicación 16,813 ms. Esto es compatible con que el handshake frío explique parte del resultado anterior, pero no prueba exactamente cuánto de los 1.041,547 ms históricos correspondió a `flush`, metadata o PID.
- La latencia reportada es **enqueue→callback observado por la aplicación**: incluye cola local, red local, respuesta del broker y planificación/despacho del callback. No es una medición de latencia interna del broker. No se midió la latencia interna.
- Consumidores usan grupo nuevo en cada lectura; se mide y excluye la espera de asignación inicial. Se registra el retardo assignment→primer mensaje; el throughput efectivo usa el intervalo primer→último mensaje recibido. También se registra assignment→último mensaje para exponer el coste inicial. IDs, secuencia, timestamp, tamaño y relleno determinista se validan por mensaje; duplicados y errores se cuentan.
- Kafka registra explícitamente por topic y en broker: `retention.ms=604800000` (7 días), `retention.bytes=-1`, `cleanup.policy=delete`, `segment.bytes=1073741824`, `segment.ms=604800000`. Retener por 7 días es configuración, no una prueba de expiración tras siete días.
- Configuración constante de cliente: `acks=all`, `compression.type=none`, idempotencia true, `linger.ms=0`, `batch.size=16384`, `max.in.flight.requests.per.connection=5`, `request.timeout.ms=30000`, `message.timeout.ms=300000`; consumidor `auto.offset.reset=earliest`, `enable.auto.commit=false`. El broker es local, un broker, una partición, RF=1.
- Los payloads se generan y envían uno por uno, no se acumulan todos en RAM. Se guarda el conjunto de secuencias recuperadas para comprobar unicidad. Preflight de disco estima `N × (payload + 256 B)` como envolvente por corrida y preserva 10 GiB libres. Se registra peak RSS del proceso.

### Piloto auditado

`results.csv`, fila `benchmark_version=0.3.1-audit`, `run_kind=validation_pilot`:

- 1.000 confirmaciones; 1.000 únicos en primera lectura; 1.000 en lectura de grupo nuevo.
- Faltantes, duplicados, pendientes y errores: 0.
- Publicación: 0,045200 s; 22.123,889 mensajes/s; 21,605360 MiB/s de payload.
- Latencia ack p50/p95/p99: 16,813 / 40,145 / 41,429 ms.
- Consumidor: asignación 63,118 ms; assignment→primer mensaje 107,105 ms; transferencia primer→último 0,006598 s (151.559,448 mensajes/s). La espera inicial se reporta aparte, no se presenta como throughput de lectura.
- Relectura: 100% tras 5,000 s desde la lectura inicial (5,177 s desde fin de publicación). Estado `ok`.

### Benchmark definitivo, nueve corridas

Los valores por repetición se conservan como filas independientes en `results.csv`. Throughput de publicación en mensajes/s; throughput de consumo usa el intervalo efectivo primer→último. Latencias en ms (p50/p95/p99 por corrida, no se promediaron).

| Eventos | Rep. | Publicación msg/s (MiB/s payload) | Consumo msg/s | Ack p50 / p95 / p99 ms | Duración pub / consumo s | Recuperados / faltantes / duplicados | Errores | Estado |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 10.000 | 1 | 46.846,793 (45,749) | 248.526,406 | 103,083 / 161,488 / 165,779 | 0,213462 / 0,040237 | 10.000 / 0 / 0 | 0 | ok |
| 10.000 | 2 | 189.032,651 (184,602) | 318.851,004 | 5,605 / 14,035 / 14,455 | 0,052901 / 0,031363 | 10.000 / 0 / 0 | 0 | ok |
| 10.000 | 3 | 175.015,939 (170,914) | 319.361,084 | 1,426 / 16,811 / 17,122 | 0,057138 / 0,031313 | 10.000 / 0 / 0 | 0 | ok |
| 100.000 | 1 | 96.180,004 (93,926) | 407.632,272 | 345,531 / 596,821 / 624,016 | 1,039717 / 0,245319 | 100.000 / 0 / 0 | 0 | ok |
| 100.000 | 2 | 202.944,813 (198,188) | 90.152,078 | 87,602 / 152,015 / 155,438 | 0,492745 / 1,109237 | 100.000 / 0 / 0 | 0 | ok |
| 100.000 | 3 | 241.308,025 (235,652) | 91.776,919 | 29,027 / 63,651 / 66,513 | 0,414408 / 1,089599 | 100.000 / 0 / 0 | 0 | ok |
| 1.000.000 | 1 | 190.933,454 (186,458) | 84.877,737 | 464,102 / 580,982 / 590,738 | 5,237427 / 11,781653 | 1.000.000 / 0 / 0 | 0 | ok |
| 1.000.000 | 2 | 243.785,361 (238,072) | 84.943,333 | 271,543 / 396,245 / 401,553 | 4,101969 / 11,772554 | 1.000.000 / 0 / 0 | 0 | ok |
| 1.000.000 | 3 | 245.647,488 (239,890) | 84.875,055 | 184,796 / 329,157 / 342,352 | 4,070874 / 11,782025 | 1.000.000 / 0 / 0 | 0 | ok |

### Estadística y recuperación

Solo las tres filas `run_kind=definitive`, status `ok`, por escenario:

| Eventos | Media publicación msg/s | Desviación estándar muestral msg/s | CV |
|---:|---:|---:|---:|
| 10.000 | 136.965,128 | 78.358,808 | 57,21% |
| 100.000 | 180.144,281 | 75.202,618 | 41,75% |
| 1.000.000 | 226.788,768 | 31.065,568 | 13,70% |

Las tres réplicas de cada escenario recuperaron 100% de los mensajes confirmados: pérdida 0%; las lecturas de retención a 5 y 30 s también recuperaron 100%, sin duplicados. El CSV incluye filas derivadas `run_kind=statistical_summary`, generadas por `--mode summarize`; SD usa n−1 y CV=SD/media. Los percentiles permanecen por corrida: no se calculó promedio ni percentil global.

### Modelo analítico Pub/Sub Lite (no medición)

Supuestos configurados: topic zonal, 1 partición, capacidad mínima configurada de publicación/suscripción 4 MiB/s por partición, almacenamiento 30 GiB. La cuota zonal sin reservation documentada (64 MiB/s publish, 128 MiB/s subscribe) no limita este escenario de 4 MiB/s.

- Cómputo: `bytes_contabilizados = 1024 + 12 = 1036 B/evento`; `eventos/s = (4 × 2^20 B/s) / 1036 B = 4048,555985`.
- Throughput contabilizado asumido: 4,000 MiB/s. Throughput de payload derivado: `4048,555985 × 1024 / 2^20 = 3,953668 MiB/s`.
- Capacidad: `30 × 2^30 / 1036 = 31.092.909 eventos`.
- El +12 B es el timestamp que la documentación añade al cálculo de **almacenamiento** para mensajes >256 B. Aplicar 1.036 B/evento al límite de throughput es un supuesto analítico explícito para diferenciar payload de bytes contabilizados; la documentación no demuestra que esa contabilidad del almacenamiento sea idéntica a la contabilidad de throughput. No se atribuyen latencias ni pérdidas.
- Fuentes oficiales: <https://cloud.google.com/pubsub/lite/docs/topics>, <https://cloud.google.com/pubsub/lite/docs/reservations> y <https://cloud.google.com/pubsub/lite/quotas>.

### Retención complementaria y recursos

Cada corrida definitiva tiene relectura con grupo nuevo a ≥5 s y otra a ≥30 s desde el final de la lectura inicial; desde el fin de publicación, la lectura de 30 s ocurrió entre 30,195 y 41,942 s (el millón de mensajes tardó ~11,78 s en lectura inicial). No demuestra retención multidiaria. El broker/topic declara retención de 7 días, pero la validación observada es de segundos.

Antes: 96 GiB libres. Después: 92 GiB libres; logs Kafka ~3,4 GiB. Memoria disponible al finalizar ~8,8 GiB; máximo RSS observado por benchmark ~482 MiB. Sin timeout, falla de integridad ni error en las nueve corridas; todas `ok`. Topics de prueba se conservan, no se borró evidencia.

### Comandos de benchmark y análisis

- Auditoría sintáctica: `.venv/bin/python -m py_compile benchmark.py`.
- Piloto auditado: `.venv/bin/python benchmark.py --mode kafka --events 1000 --size-bytes 1024 --repetitions 1 --bootstrap-servers 127.0.0.1:9092 --retention-delay-seconds 5 --long-retention-delay-seconds 0 --timeout-s 300 --output results.csv`.
- Definitivo: `.venv/bin/python benchmark.py --mode kafka --events 10000 100000 1000000 --size-bytes 1024 --repetitions 3 --bootstrap-servers 127.0.0.1:9092 --retention-delay-seconds 5 --long-retention-delay-seconds 30 --timeout-s 300 --reserve-disk-gib 10 --kafka-data-dir data/kafka/logs --kafka-retention-ms 604800000 --kafka-retention-bytes -1 --kafka-segment-bytes 1073741824 --kafka-segment-ms 604800000 --batch-size-bytes 16384 --output results.csv`.
- Modelo: `.venv/bin/python benchmark.py --mode pubsublite-analytical --events 10000 100000 1000000 --partitions 1 --storage-gib 30 --model-publish-mib-per-s 4 --model-subscribe-mib-per-s 4 --size-bytes 1024 --output results.csv`.
- Estadística: `.venv/bin/python benchmark.py --mode summarize --output results.csv`.
- Se consultó la configuración efectiva del topic con `kafka-configs.sh --describe`; coincidió con la retención documentada arriba.

### Limitaciones y cautelas

Broker local único y RF=1: no mide tolerancia a fallas ni replicación distribuida. Productor, consumidor y broker comparten CPU, disco y host; no hay red WAN, por lo que no es comparación de latencia de red. Variación alta en 10k y 100k (CV 57,21%/41,75%) se reporta tal cual, sin filtrar corridas válidas. Tres repeticiones son una muestra pequeña. La cifra del consumidor es velocidad de transferencia después del primer registro; el startup/assignment/fetch se reporta aparte. El modelo Pub/Sub Lite es solo un límite condicionado por capacidad documentada, no resultado del servicio real. Retención solo comprobada durante 5 y 30 s, no durante días. No se usaron servicios Google de pago, no se publicó el repositorio ni se redactó el artículo.

**Cierre de trazabilidad:** `results.csv` quedó con 25 filas (incluye historia previa, intento fallido, pilotos, 9 corridas definitivas, 6 filas analíticas etiquetadas y 3 resúmenes estadísticos). Las instantáneas previas están bajo `data/`, excluida localmente de Git. Se reinició Kafka de forma controlada para aplicar la retención estática; sus logs confirman shutdown graceful y el estado del productor histórico, y los datos persistieron. No se hizo commit.

## Etapa 4 — artículo reproducible y preparación de entrega

**Fecha:** 2026-11-14  
**Modelo utilizado:** GPT-6 Luna (OpenAI), según la configuración de la sesión.  
**Datos:** exclusivamente las filas y estimaciones existentes en `results.csv`; no se volvió a ejecutar el benchmark ni se modificó el CSV.

### Prompt recibido

> Apruebo la etapa 3. Continuemos con la ETAPA 4: elaboración del artículo científico reproducible y preparación de los entregables.
>
> Autor: Alan David Monar Benalcazar; ESPOCH, Ingeniería en Telemática, Octavo PAO; asignatura Sistemas Distribuidos y Paradigmas de la Computación (ETL1TP50), Unidad 2: Comunicación y Coordinación en Sistemas Distribuidos; fecha 14 de noviembre de 2026.
>
> Título: “Evaluación experimental del throughput y retención de mensajes en Apache Kafka frente a un modelo analítico de Cloud Pub/Sub Lite”.
>
> Revisar benchmark.py, results.csv y este registro; utilizar solo resultados existentes; crear articulo.qmd y _quarto.yml IEEEtran conference, 3–4 páginas, doble columna; redactar el artículo en español con resumen, palabras clave, introducción, metodología, resultados, discusión, conclusiones y referencias IEEE. Incluir las estadísticas de nueve corridas, throughput de consumo, percentiles por repetición, recuperación íntegra de 3.330.000 eventos y análisis de latencia del piloto original frente al auditado. Generar figuras/tablas desde results.csv con bloques Quarto/Python y referencias cruzadas.
>
> Explicar el modelo de Pub/Sub Lite (4 MiB/s, 1 partición zonal, 30 GiB, 1.036 B bajo supuesto, sensibilidad de 1.024 B, 31.092.909 eventos) y su cierre el 2027-01-31 y cierre a nuevos clientes desde septiembre de 2024. No presentarlo como medición ni comparar el límite con rendimiento real de Kafka.
>
> Crear references.bib con 5–8 referencias verificadas, separando literatura científica de documentación técnica; README reproducible desde clon limpio con versiones, comandos, arquitectura y diccionario de CSV; actualizar trazabilidad y declaración de integridad académica. Compilar PDF y verificar páginas, IEEE/doble columna, referencias, tablas, figuras, citas, concordancia con datos y reproducibilidad. El repositorio público debe contener exactamente ocho archivos: articulo.pdf, articulo.qmd, references.bib, _quarto.yml, benchmark.py, results.csv, README.md y PROMPTS_OPENCODE.md. Mantener herramientas, logs, entorno, datos y respaldos fuera de Git. No publicar, no repetir el benchmark, no alterar resultados y detenerse para revisión del autor.

### Trabajo realizado y verificaciones

- Se revisaron las 25 filas y 82 columnas de `results.csv`. Los cálculos del artículo se derivan solo de las nueve filas `0.3.1-audit / definitive / ok`; las filas de piloto, intento fallido, resúmenes y estimaciones no se combinaron. No se editó `results.csv` ni se repitieron pruebas.
- Se crearon `articulo.qmd`, `_quarto.yml`, `references.bib` y `README.md`. Las dos figuras son celdas Python etiquetadas en el QMD; leen las filas definitivas del CSV y quedan en el directorio temporal de render, no como archivos gráficos en la raíz. El gráfico de publicación presenta los nueve datos y media ± SD muestral; el de p95 muestra puntos independientes por repetición. Se eliminaron las tablas Pandoc largas porque `longtable` no compila en el modo de dos columnas de IEEEtran; se cambiaron por tablas LaTeX compactas con referencias.
- La afiliación, carrera, asignatura, unidad y fecha se incorporaron al bloque de título mediante un ajuste de `\maketitle` en el encabezado LaTeX del QMD; se verificó visualmente que no se desborda y mantiene el formato IEEEtran.
- El artículo conserva 3 páginas tamaño carta, clase `IEEEtran`, opción `conference`, con composición a dos columnas y cinco trabajos científicos más dos fuentes técnicas. Se verificó visualmente cada página, se extrajo el texto del PDF para comprobar las cifras y se revisaron las referencias numéricas y citas. No se detectaron marcadores `??` en referencias cruzadas de figuras/tablas ni citas sin resolver. Las advertencias de Quarto sobre referencias nativas de tablas LaTeX son esperadas: las dos tablas usan etiquetas y referencias LaTeX explícitas.
- Bibliografía científica contrastada con Crossref/DOI y/o OpenAlex, y páginas de publicación:
  - Wu, Shang y Wolter, “Performance Prediction for the Apache Kafka Messaging System”, IEEE HPCC/SmartCity/DSS 2019, pp. 154–161, DOI `10.1109/HPCC/SmartCity/DSS.2019.00036`.
  - Raza et al., “Benchmarking Apache Kafka under Network Faults”, Middleware 2021 Demos and Posters, pp. 5–7, DOI `10.1145/3491086.3492470`.
  - Akidau et al., “The Dataflow Model…”, PVLDB, vol. 8, n.º 12, 2015, pp. 1792–1803, DOI `10.14778/2824032.2824076`.
  - Zaharia et al., “Discretized Streams…”, SOSP 2013, pp. 423–438, DOI `10.1145/2517349.2522737`.
  - Raptis y Passarella, “A Survey on Networked Data Streaming With Apache Kafka”, IEEE Access, vol. 11, 2023, pp. 85333–85350, DOI `10.1109/ACCESS.2023.3303810`.
- Las dos fuentes técnicas —no publicadas como literatura indexada— son la documentación de Pub/Sub Lite de Google Cloud y la documentación oficial Apache Kafka 4.4. Se comprobó el aviso de Google: cierre el 31-01-2027 y nuevos clientes no admitidos desde el 24-09-2024. Las capacidades zonales, almacenamiento mínimo y cálculo de 12 bytes para almacenamiento se contrastaron con páginas oficiales de temas y cuotas.
- Se resolvieron problemas reales de compilación: `geometry: false` no era un valor YAML permitido y se retiró; se restringió `project.render` a `articulo.qmd` porque el directorio local `tools/` contiene QMD de ejemplo; se seleccionó `natbib` numérico con `IEEEtran`; y se cambiaron expresiones matemáticas al delimitador `$...$`. El render generó y compiló las dos figuras sin versionarlas.
- La instalación local de TinyTeX no encontró el archivo de fuente `TUptm.fd`; XeLaTeX aplicó su sustitución de fuente disponible y la compilación terminó correctamente. No se alteró el tamaño de papel, márgenes ni tamaño base para ajustar páginas.
- Se consultó fecha, página y tamaño del PDF con `pdfinfo`; se renderizaron imágenes temporales de las tres páginas para inspección visual y se extrajo texto con `pdftotext`. Se verificaron número de páginas, doble columna, tablas/figuras dentro del área, integridad de cifras, citas, referencias y ausencia de referencias `??`.
- README documenta Kafka/JDK/Python/Quarto/TeX, dependencias Python fijadas, configuración KRaft local, espacio de disco, comandos reales del benchmark (solo como reproducción futura y no ejecutados en esta etapa), modos analítico/resumen, compilación y grupos de columnas. El PDF se compila en `/tmp`; la entrega final copia únicamente el PDF a la raíz. El repo no se publicó ni se hizo commit.

### Declaración de integridad académica

OpenCode, con el modelo GPT-6 Luna, fue utilizado como asistente de programación, búsqueda y verificación bibliográfica, maquetación y análisis reproducible de datos ya existentes. La herramienta no ejecutó nuevas corridas en la Etapa 4 ni creó mediciones. El autor conserva la responsabilidad de revisar el artículo, verificar su interpretación, validar las fuentes y cifras, cumplir las normas académicas de ESPOCH y aprobar la versión final. Esta declaración no atribuye al autor intervenciones de revisión que no hayan ocurrido todavía.

### Revisión editorial final asistida por ChatGPT

**Fecha:** 2026-11-14  
**Alcance:** correcciones editoriales solicitadas por el autor sobre `articulo.qmd`, `references.bib`, `README.md` y este registro. No se repitió el benchmark y no se modificó `results.csv`.

- Se documentaron en metodología el hardware y las versiones verificadas en los registros locales; se especificó Kafka 4.4.0 en modo KRaft.
- Se rediseñó la Tabla I como tabla IEEE de ancho completo con cinco columnas explícitas, estadísticas redondeadas y recuperación; se compactó la Tabla II. Se ajustaron las figuras manteniendo las nueve observaciones, la media y la desviación estándar; la leyenda de publicación se movió a una zona sin puntos.
- Se configuró Liberation Serif, fuente métrica y visualmente compatible con Times/IEEE disponible en el entorno, mediante XeLaTeX; no se cambiaron márgenes ni tamaño de texto para forzar la paginación.
- Se corrigió la URL de Kafka 4.4 a `https://kafka.apache.org/44/` y se enumeraron los enlaces oficiales de Google Cloud para topics, quotas y reservations, manteniendo siete referencias.
- Se redujeron repeticiones sobre Pub/Sub Lite y la conclusión ahora expresa compatibilidad con la hipótesis de aumento de throughput, sin afirmar causalidad. La explicación del piloto se limita al mensaje registrado `Coordinator load in progress`; no adjudica el reintento al PID.
- Se corrigieron en README la preparación KRaft desde clon limpio y el comando de render para no depender de Quarto dentro de `tools/`; se documentaron Liberation Serif y las versiones reales del equipo.
- Recompilación final con Quarto/XeLaTeX: PDF IEEEtran conference, doble columna, 3 páginas tamaño carta. Las tres páginas se revisaron visualmente; la afiliación queda centrada bajo el autor, tablas y figuras son legibles y no se observaron desbordes ni solapamientos. `pdffonts` confirma Liberation Serif embebida para el texto principal. Se mantienen siete referencias y no quedan citas sin resolver; Quarto emite únicamente el aviso esperado de que la etiqueta de Tabla II es LaTeX nativa y no una referencia cruzada Quarto.
- Integridad: SHA-256 de `results.csv` antes y después `1da12d006dd135b932dd73d91e7cadb244c1fb5960f51091673ce407499e0c2f`; sin modificaciones. No se repitió el benchmark ni se publicó el repositorio.

## Etapa 5 — auditoría final y publicación

**Fecha:** 2026-11-14  
**Asistencia editorial:** ChatGPT participó en la revisión editorial final descrita en la sección anterior. OpenCode realizó las verificaciones locales y de publicación registradas aquí; no se atribuyen a ChatGPT las acciones de GitHub.

### Auditoría previa a publicación

- Se verificó la presencia y el tipo de los ocho entregables requeridos. Una búsqueda de credenciales, claves privadas, correos/teléfonos y rutas absolutas del equipo no encontró coincidencias en los archivos publicables. Se conserva el nombre del autor y la afiliación por ser datos bibliográficos necesarios.
- El PDF final conserva 3 páginas, tamaño Letter (612 × 792 pt), configuración `IEEEtran` conference a dos columnas, citas/referencias resueltas y fuentes Liberation Serif embebidas.
- El SHA-256 actual de `results.csv` coincide con el valor autorizado: `1da12d006dd135b932dd73d91e7cadb244c1fb5960f51091673ce407499e0c2f`.
- Se creó una copia temporal limpia con exclusivamente los ocho archivos entregables. Desde ella se renderizó el artículo a PDF con Quarto 1.11.9, TeX Live 2026/XeLaTeX e `IEEEtran`, y un entorno Python independiente con las versiones fijadas en README. No se usaron `data/`, `tools/` ni `.venv` del proyecto para la compilación. El PDF reproducido también resultó de 3 páginas tamaño carta, con citas resueltas. `benchmark.py --help` confirmó los modos Kafka, analítico y resumen disponibles; no se ejecutó ninguno ni se repitió el benchmark.
- Durante la auditoría se constató que Ubuntu puede carecer de `ensurepip`; se documentó en README el requisito `python3.14-venv` y una alternativa con el bootstrap oficial de pip, sin añadir artefactos al repositorio.
- En la comprobación previa, GitHub CLI autenticó la cuenta `Alan-m-04` y la API devolvió 404 para `Alan-m-04/investigacion-streaming-u2`, por lo que no se encontró repositorio previo. El árbol local estaba en `master`, sin commits; después se renombró a `main`. El archivo `.gitignore` local se mantuvo fuera de los entregables y no se añadió.

### Verificación posterior a la publicación

- Se creó `https://github.com/Alan-m-04/investigacion-streaming-u2` como repositorio público con la descripción autorizada. La rama predeterminada y publicada es `main`; no se sobrescribió contenido previo ni se usó force push.
- El commit inicial `6125b9eed2515db63bd6fa9fe4bb60f42d3da44d` publicó exactamente los ocho archivos obligatorios. La API pública del árbol de `main` devolvió esos ocho y ningún archivo adicional; `.gitignore` no se versionó.
- La página del repositorio y la página de README respondieron HTTP 200 sin autenticación; la vista web muestra el README y los ocho archivos. Se descargó `articulo.pdf` desde el enlace público y `pdfinfo` confirmó 3 páginas tamaño carta. Se descargó también el CSV público: su SHA-256 coincide exactamente con el original.
- El repositorio puede consultarse sin iniciar sesión. El benchmark no se repitió; la única verificación del programa fue `--help`. No se modificó ningún otro repositorio.
