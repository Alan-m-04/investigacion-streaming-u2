# Evaluación de throughput y retención de mensajes

**Título:** *Evaluación experimental del throughput y retención de mensajes en Apache Kafka frente a un modelo analítico de Cloud Pub/Sub Lite*  
**Autor:** Alan David Monar Benalcazar  
**Institución:** Escuela Superior Politécnica de Chimborazo (ESPOCH)  
**Carrera:** Ingeniería en Telemática, Octavo PAO  
**Asignatura:** Sistemas Distribuidos y Paradigmas de la Computación (ETL1TP50)  
**Unidad:** 2 — Comunicación y Coordinación en Sistemas Distribuidos  
**Fecha:** 14 de noviembre de 2026

## Resumen

Se midieron throughput de publicación y consumo y recuperación de mensajes en un broker local Apache Kafka 4.4.0, con cargas de 10.000, 100.000 y 1.000.000 de eventos de 1.024 bytes, en tres repeticiones por escenario. Las nueve corridas recuperaron 3.330.000 de 3.330.000 eventos sin faltantes ni duplicados. El contraste de Cloud Pub/Sub Lite es exclusivamente analítico: se calculó un límite condicionado por capacidad configurada, no el rendimiento del servicio. Pub/Sub Lite está anunciado para cierre el 31 de enero de 2027 y no admite nuevos clientes desde septiembre de 2024. Este estudio no permite afirmar superioridad entre ambas plataformas.

## Contenido del repositorio

La entrega pública contiene exactamente estos ocho archivos:

| Archivo | Función |
|---|---|
| `articulo.pdf` | Artículo final en IEEE Conference |
| `articulo.qmd` | Fuente del artículo y bloques Python que leen el CSV y generan figuras |
| `references.bib` | Cinco trabajos científicos y dos fuentes técnicas oficiales |
| `_quarto.yml` | Configuración Quarto/IEEEtran conference |
| `benchmark.py` | Benchmark auditado, modelo analítico y resumen estadístico |
| `results.csv` | Datos históricos, pilotos, fallo, mediciones, modelo y resúmenes identificados |
| `README.md` | Instrucciones, metodología y descripción de datos |
| `PROMPTS_OPENCODE.md` | Trazabilidad de etapas y declaración de integridad académica |

Las instalaciones, logs, broker, topics, instantáneas históricas y entorno virtual son artefactos locales y no forman parte de los ocho archivos de entrega.

## Diseño y arquitectura

Kafka 4.4.0 se ejecutó en un broker local con una partición y RF=1. Productor, consumidor y broker comparten host. Se enviaron payloads deterministas de 1.024 bytes. Las nueve corridas definitivas comprenden tres tamaños por tres repeticiones, con topics e identificadores únicos. El productor emplea `acks=all`, compresión `none`, idempotencia, `linger.ms=0`, `batch.size=16384`, `max.in.flight.requests.per.connection=5`, `request.timeout.ms=30000` y `message.timeout.ms=300000`. El benchmark separa calentamiento, preparación y asignación de partición de los intervalos de transferencia; valida IDs y payloads, y realiza lecturas con grupos nuevos.

Los topics configuran `retention.ms=604800000` (siete días), `retention.bytes=-1`, `cleanup.policy=delete`, `segment.bytes=1073741824` y `segment.ms=604800000`. La recuperación observada a 5 y 30 s no equivale a probar expiración a siete días. `acks=all` con RF=1 tampoco prueba durabilidad distribuida.

Pub/Sub Lite no fue ejecutado. El modelo asume un topic zonal, una partición con 4 MiB/s de capacidad configurada y 30 GiB. Con 1.036 bytes por evento (1.024 payload + 12 timestamp, aplicando al throughput una sobrecarga documentada para almacenamiento) resulta 4.048,56 eventos/s y una capacidad teórica de 31.092.909 eventos. Si el throughput cuenta solo los 1.024 bytes de payload, el límite es 4.096 eventos/s. No se estiman latencia, pérdida ni recuperación. Google anunció su retirada para el 31 de enero de 2027 y la indisponibilidad para nuevos clientes desde el 24 de septiembre de 2024.

## Resultados principales

| Eventos | Throughput medio de publicación (msg/s) | SD muestral (msg/s) | CV | Recuperación |
|---:|---:|---:|---:|---:|
| 10.000 | 136.965,128 | 78.358,808 | 57,21 % | 100 % |
| 100.000 | 180.144,281 | 75.202,618 | 41,75 % | 100 % |
| 1.000.000 | 226.788,768 | 31.065,568 | 13,70 % | 100 % |

Cada una de las nueve pruebas recuperó todos los eventos, sin faltantes, duplicados ni errores; total publicado: 3.330.000. La variabilidad en 10.000 y 100.000 fue alta y se conserva sin filtrar. No existe comparación experimental directa con el servicio Google.

## Requisitos y versiones

Entorno registrado del artículo: Ubuntu 26.04.1 LTS, Intel Core i7-1165G7 (4 núcleos/8 hilos), 14 GiB RAM y SSD NVMe; Java Temurin 21.0.13+9; Python 3.14.4; Quarto 1.11.9; Apache Kafka 4.4.0 en KRaft; `confluent-kafka` 2.16.1; Jupyter 1.1.1; `ipykernel` 7.4.0; Matplotlib 3.11.2; NumPy 2.5.4; TeX Live 2026 con `IEEEtran`. El análisis de figuras utiliza la biblioteca estándar `csv` y Matplotlib. Para nuevas corridas, el cliente Kafka usado fue `confluent-kafka`.

Quarto, TeX Live, Java, Kafka y `.venv` no se incluyen en la entrega. En Ubuntu, instale Java 21, Python 3.14 y `python3.14-venv`, Quarto 1.11.9 y una distribución TeX con `IEEEtran`; instale también Liberation Serif (paquete `fonts-liberation`). Asegúrese de que `java`, `quarto`, `xelatex` y `kpsewhich` estén disponibles en `PATH`; compruebe `quarto --version`, `xelatex --version` y `kpsewhich IEEEtran.cls`. Prepare las dependencias Python desde la raíz del clon:

```bash
sudo apt install python3.14-venv
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install confluent-kafka==2.16.1 jupyter==1.1.1 \
  ipykernel==7.4.0 matplotlib==3.11.2 numpy==2.5.4
python -m ipykernel install --user --name streaming-u2 --display-name "Python (streaming-u2)"
```

Si la distribución de Python no incluye `ensurepip` y no se puede instalar `python3.14-venv`, use el bootstrap oficial de pip dentro de un entorno virtual sin pip:

```bash
python3 -m venv --without-pip .venv
curl -fL https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
.venv/bin/python /tmp/get-pip.py
.venv/bin/python -m pip install confluent-kafka==2.16.1 jupyter==1.1.1 \
  ipykernel==7.4.0 matplotlib==3.11.2 numpy==2.5.4
```

Se recomienda Java 17 o superior por el requisito de Kafka 4.4.0. La versión del kernel debe estar disponible como `python3` para Quarto; alternativamente, configure `QUARTO_PYTHON` para el Python del entorno.

## Preparar Kafka desde un clon limpio

Descargue la distribución binaria oficial Kafka 4.4.0 y extráigala en la ruta local ignorada `tools/` (estos comandos preparan esa ruta desde un clon limpio). Desde la raíz del proyecto, cree directorios y una configuración KRaft local:

```bash
mkdir -p tools data/kafka/logs data/kafka/metadata
curl -fL https://downloads.apache.org/kafka/4.4.0/kafka_2.13-4.4.0.tgz \
  -o /tmp/kafka_2.13-4.4.0.tgz
tar -xzf /tmp/kafka_2.13-4.4.0.tgz -C tools
ln -sfn kafka_2.13-4.4.0 tools/kafka
cat > data/kafka/server.properties <<'EOF'
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=data/kafka/logs
metadata.log.dir=data/kafka/metadata
num.partitions=1
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.ms=604800000
log.retention.bytes=-1
log.cleanup.policy=delete
log.segment.bytes=1073741824
log.segment.ms=604800000
EOF
export PATH="$PWD/tools/kafka/bin:$PATH"
CLUSTER_ID="$(kafka-storage.sh random-uuid)"
kafka-storage.sh format --no-initial-controllers -t "$CLUSTER_ID" \
  -c data/kafka/server.properties
kafka-server-start.sh data/kafka/server.properties
```

Mantenga el broker en ejecución en esa terminal. En otra terminal, desde la raíz del proyecto, compruebe conectividad con:

```bash
tools/kafka/bin/kafka-cluster.sh api-versions --bootstrap-server 127.0.0.1:9092
```

El benchmark comprueba que haya 10 GiB de espacio libre además del espacio estimado para la corrida. Si el disco no cumple esa reserva, no reduzca la protección a ciegas; inspeccione el almacenamiento y use `--reserve-disk-gib` solo con una decisión explícita. Los topics de prueba no se borran automáticamente.

## Reproducir las corridas

**Nota:** el comando siguiente vuelve a ejecutar mediciones y agrega filas; no es necesario para reconstruir el artículo ni para reproducir los resultados ya entregados. No lo ejecute sobre el `results.csv` de la investigación si desea conservarlo sin cambios. Para una repetición independiente, copie el CSV a otro nombre y use `--output` con esa ruta:

```bash
cp results.csv /tmp/results-reproduction.csv
.venv/bin/python benchmark.py --mode kafka \
  --events 10000 100000 1000000 --size-bytes 1024 --repetitions 3 \
  --bootstrap-servers 127.0.0.1:9092 \
  --retention-delay-seconds 5 --long-retention-delay-seconds 30 \
  --timeout-s 300 --reserve-disk-gib 10 \
  --kafka-data-dir data/kafka/logs \
  --kafka-retention-ms 604800000 --kafka-retention-bytes -1 \
  --kafka-segment-bytes 1073741824 --kafka-segment-ms 604800000 \
  --batch-size-bytes 16384 --output /tmp/results-reproduction.csv
```

El programa detiene la secuencia ante una corrida no válida. Para ejecutar solo el cálculo (sin Google Cloud):

```bash
.venv/bin/python benchmark.py --mode pubsublite-analytical \
  --events 10000 100000 1000000 --partitions 1 --storage-gib 30 \
  --model-publish-mib-per-s 4 --model-subscribe-mib-per-s 4 \
  --size-bytes 1024 --output /tmp/results-reproduction.csv
```

Las estadísticas se derivan solo de tres filas definitivas válidas por escenario:

```bash
.venv/bin/python benchmark.py --mode summarize \
  --output /tmp/results-reproduction.csv
```

## Generar figuras y PDF

El documento Quarto lee `results.csv` y genera las figuras en el render; no se versionan imágenes. Verifique que el kernel tenga Matplotlib y ejecute desde la raíz:

```bash
QUARTO_PYTHON="$PWD/.venv/bin/python" \
  quarto render articulo.qmd --to pdf --output-dir /tmp/streaming-u2-build
cp /tmp/streaming-u2-build/articulo.pdf ./articulo.pdf
```

El comando de compilación genera `.tex` y recursos temporales en `/tmp`, no en la raíz. Se requiere `IEEEtran.cls`, XeLaTeX y BibTeX/estilo IEEEtran instalados. La configuración usa `IEEEtran` con opción `conference` y el PDF resultante debe mantenerse en cuatro páginas o menos.

## `results.csv`: procedencia y columnas

Cada fila está etiquetada por `origin`, `run_kind`, `status` y `benchmark_version`:

- **Medición histórica y piloto:** filas sin versión o `validation_pilot`; no se usan para estadística definitiva.
- **Intento fallido:** `status=failed_before_publish`; se conserva la evidencia de que no publicó eventos.
- **Definitivas:** nueve filas `benchmark_version=0.3.1-audit`, `run_kind=definitive`, `status=ok`; únicas filas usadas en la estadística y figuras del artículo.
- **Modelo:** `origin=estimacion_analitica_no_medicion` y `run_kind=analytical_capacity_bound` cuando está presente; no corresponde a ejecución Pub/Sub Lite.
- **Resúmenes:** `run_kind=statistical_summary`; valores derivados, no observaciones adicionales.

Las columnas se agrupan así:

| Grupo de columnas | Campos |
|---|---|
| Identificación y procedencia | `benchmark_version`, `timestamp_utc`, `run_kind`, `origin`, `system`, `run_id`, `topic`, `repetition`, `status`, `historical_record_note` |
| Carga y configuración | `events_requested`, `payload_bytes`, `partitions`, `replication_factor`, `acks`, `compression`, `linger_ms`, `batch_size_bytes`, `idempotence`, `max_in_flight`, `effective_config` |
| Retención/configuración Kafka | `retention_ms`, `retention_bytes`, `cleanup_policy`, `segment_bytes`, `segment_ms`, `retention_delay_s`, `long_retention_delay_s` |
| Preparación y tiempos | `topic_creation_ms`, `producer_init_ms`, `metadata_ms`, `warmup_topic`, `warmup_total_ms`, `warmup_ack_ms`, `consumer_init_ms`, `consumer_assignment_ms`, `consumer_first_message_ms`, `publish_duration_s`, `consumer_duration_s`, `consumer_post_assignment_s` |
| Throughput y latencia | `publish_messages_per_s`, `publish_mib_per_s`, `consume_messages_per_s`, `consume_mib_per_s`, `ack_latency_p50_ms`, `ack_latency_p95_ms`, `ack_latency_p99_ms` |
| Integridad y recuperación | `confirmed_events`, `unconfirmed_events`, `primary_recovery_percent`, `unique_recovered`, `missing_events`, `duplicate_events`, `error_count`, `errors_detail`, `retention_unique_recovered`, `retention_missing_events`, `retention_elapsed_from_publish_s`, `retention_elapsed_from_initial_consume_s`, `long_retention_unique_recovered`, `long_retention_missing_events`, `long_retention_elapsed_from_publish_s`, `long_retention_elapsed_from_initial_consume_s`, `peak_rss_mib` |
| Modelo Pub/Sub Lite | `model_publish_capacity_mib_per_s`, `model_subscribe_capacity_mib_per_s`, `model_event_storage_bytes`, `model_retention_capacity_events`, `model_retention_capacity_gib`, `model_max_publish_events_per_s`, `model_max_subscribe_events_per_s`, `model_payload_publish_mib_per_s`, `model_payload_subscribe_mib_per_s`, `model_assumptions`, `model_sources` |
| Resumen estadístico | `throughput_mean_messages_per_s`, `throughput_sample_sd_messages_per_s`, `throughput_cv_percent`, `recovery_percent`, `loss_percent`, `retention_5s_recovery_percent`, `retention_30s_recovery_percent` |

Los campos sin valor en una fila significan que no aplican o no se midieron; no deben interpretarse como cero. La latencia es enqueue→callback observada por la aplicación y no latencia interna del broker. El throughput de consumo excluye la espera de asignación y el primer mensaje; ambas se informan en campos separados.

## Referencias y limitaciones

`references.bib` contiene cinco trabajos científicos con metadatos y DOI verificados, y dos fuentes técnicas oficiales de Google Cloud y Apache Kafka; las fuentes técnicas no se presentan como publicaciones indexadas. Véase `PROMPTS_OPENCODE.md` para la trazabilidad y decisiones de integridad.

Limitaciones: broker local único, RF=1, sin WAN, sin réplica distribuida ni prueba de fallos; productor, consumidor y broker comparten recursos; solo tres repeticiones; retención comprobada experimentalmente por segundos; modelo Pub/Sub Lite analítico, no servicio medido. Los CV altos de las cargas pequeña y mediana se conservan. No se establece comparación directa ni superioridad entre plataformas.
