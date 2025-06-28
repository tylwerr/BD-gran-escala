# Apache beam examples

## Create a Dag png file

```
python src/in_memory.py --runner apache_beam.runners.render.RenderRunner --render_output dag.png
```

## Create a interactive Dag

```
python src/in_memory.py --runner apache_beam.runners.render.RenderRunner --render_port 8089
```


# Run generators:

python -m src.generator.generator --generator_type race_event

python -m src.generator.generator --generator_type helicopter_teams

python -m src.generator.generator --generator_type telemetry

python -m src.generator.generator --generator_type fan_engagement

python -m src.generator.generator --generator_type race_event 


## Crear lista de equipos en Minio bucket
python -m src.generator.generator \
    --generator_type helicopter_teams \
    --output minio \
    --num_teams 10 \
    --race_id cup_jun_2025 \
    --output_path cup_jun_2025/helicopter_teams/teams.json


## Crear datos de carrera y publicar en Kafka
python -m src.generator.generator \
    --generator_type race_event \
    --output kafka \
    --num_teams 10 \
    --race_id cup_jun_2025 \
    --num_records 100 \
    --output_path race_events_topic

## Consumir datos de carrera, guardarlos en un Minio bucket y enviarlos al sistema de telemetria
python -m src.beam.race_events_processor \
    --runner PrismRunner  \
    --output_bucket race-event-bucket  \
    --output_file_prefix cup_jun_2025/events/race_events  \
    --num_records 100 \
    --input_topic race_events_topic \
    --output_topic telemetry_events_topic

## Crear datos de telemetria y publicar en Kafka
python -m src.generator.generator \
    --generator_type telemetry \
    --output kafka \
    --num_teams 10 \
    --race_id cup_jun_2025 \
    --num_records 100 \
    --output_path telemetry_topic

## Consumir datos de eventos de carrera y telemetria y guardar en local
python -m src.beam.telemetry_processor_file \
    --runner PrismRunner  \
    --output_bucket output  \
    --output_file_prefix cup_jun_2025/telemetry/  \
    --num_records 100 \
    --input_events_topic telemetry_events_topic \
    --input_telemetry_topic telemetry_topic 

## Consumir datos de eventos de carrera y telemetria y guardar en bucket
python -m src.beam.telemetry_processor_file \
    --runner PrismRunner  \
    --output_bucket s3://ai-bucket  \
    --output_file_prefix cup_jun_2025/telemetry/  \
    --num_records 100 \
    --input_events_topic telemetry_events_topic \
    --input_telemetry_topic telemetry_topic 

## Consumir datos de eventos de carrera y telemetria y guardar en redis y postgresql
python -m src.beam.telemetry_processor \
    --runner PrismRunner  \
    --output_bucket s3://ai-bucket  \
    --output_file_prefix cup_jun_2025/telemetry/  \
    --num_records 100 \
    --input_events_topic telemetry_events_topic \
    --input_telemetry_topic telemetry_topic \
    --output_table_name telemetry_data_table
