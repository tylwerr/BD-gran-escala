import pyarrow as pa
import pyarrow.parquet as pq

# DEFINIR ESQUEMA
schema = pa.schema([
    ("vehicle_id", pa.string()),
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("gps_latitude", pa.float64()),
    ("gps_longitude", pa.float64()),
    ("engine_rpm", pa.int32()),
    ("engine_load_percentage", pa.int32()),
    ("fuel_level_percentage", pa.int32()),
    ("fuel_consumption_rate_l_hr", pa.float64()),
    ("speed_km_h", pa.float64()),
    ("odometer_km", pa.float64()),
    ("operating_hours", pa.float64()),
    ("hydraulic_pressure_psi", pa.int32()),
    ("hydraulic_fluid_temp_c", pa.int32()),
    ("engine_coolant_temp_c", pa.int32()),
    ("battery_voltage_v", pa.float64()),
    ("gear_engaged", pa.string()),
    ("accelerator_pedal_position_percentage", pa.int32()),
    ("brake_pedal_position_percentage", pa.int32()),
    ("error_code", pa.string()),
    ("check_engine_light", pa.string()),
    ("low_oil_pressure", pa.string()),
    ("engine_temp_c", pa.int32()),
    ("transmission_temp_c", pa.int32()),
    ("hydraulic_system_temp_c", pa.int32()),
    ("vibration_index_x", pa.float64()),
    ("vibration_index_y", pa.float64()),
    ("vibration_index_z", pa.float64()),
    ("implement_id", pa.string()),
    ("section1_status", pa.string()),
    ("section2_status", pa.string()),
    ("section3_status", pa.string()),
    ("flow_rate_l_min", pa.float64()),
    ("seed_rate_seeds_per_sq_m", pa.float64()),
    ("yield_moisture_content_percentage", pa.float64()),
    ("yield_mass_flow_rate_kg_s", pa.float64()),
    ("yield_rate_per_hectare_kg_ha", pa.float64()),
    ("soil_moisture_percentage", pa.float64()),
    ("soil_temperature_c", pa.float64()),
    ("nitrogen_ppm", pa.int32()),
    ("phosphorus_ppm", pa.int32()),
    ("potassium_ppm", pa.int32())
])
