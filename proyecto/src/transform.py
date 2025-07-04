from dateutil import parser
from datetime import timezone

def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def parse_timestamp(ts_str):
    try:
        dt = parser.isoparse(ts_str)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

#Limpieza y validación
def transform_data(record):
    diagnostic = record.get("Diagnostic_Data") or {}
    warning = diagnostic.get("Warning_Lights") or {}
    temps = diagnostic.get("Component Temperatures_C") or {}
    vib = diagnostic.get("Vibration") or {}

    implement = record.get("Implement") or {}
    section = implement.get("Section_Control_Status") or {}
    yield_data = implement.get("Yield_Monitor_Data") or {}
    soil = implement.get("Soil_Sensor_Data") or {}
    nutrients = soil.get("Soil_Nutrient_Levels") or {}

    return {
        "vehicle_id": record.get("Vehicle_ID"),
        "timestamp": parse_timestamp(record.get("Timestamp")),
        "gps_latitude": safe_float(record["GPS_Coordinates"]["Latitude"]),
        "gps_longitude": safe_float(record["GPS_Coordinates"]["Longitude"]),
        "engine_rpm": safe_int(record.get("Engine_RPM")),
        "engine_load_percentage": safe_int(record.get("Engine_Load_Percentage")),
        "fuel_level_percentage": safe_int(record.get("Fuel_Level_Percentage")),
        "fuel_consumption_rate_l_hr": safe_float(record.get("Fuel_Consumption_Rate_L_hr")),
        "speed_km_h": safe_float(record.get("Speed_km_h")),
        "odometer_km": safe_float(record.get("Odometer_km")),
        "operating_hours": safe_float(record.get("Operating_Hours")),
        "hydraulic_pressure_psi": safe_int(record.get("Hydraulic_Pressure_psi")),
        "hydraulic_fluid_temp_c": safe_int(record.get("Hydraulic_Fluid_Temp_C")),
        "engine_coolant_temp_c": safe_int(record.get("Engine_Coolant_Temp_C")),
        "battery_voltage_v": safe_float(record.get("Battery_Voltage_V")),
        "gear_engaged": record.get("Gear_Engaged"),
        "accelerator_pedal_position_percentage": safe_int(record.get("Accelerator_Pedal_Position_Percentage")),
        "brake_pedal_position_percentage": safe_int(record.get("Brake_Pedal_Position_Percentage")),
        "error_code": diagnostic.get("Error_Code"),
        "check_engine_light": warning.get("CheckEngineLight"),
        "low_oil_pressure": warning.get("LowOilPressure"),
        "engine_temp_c": safe_int(temps.get("Engine")),
        "transmission_temp_c": safe_int(temps.get("Transmission")),
        "hydraulic_system_temp_c": safe_int(temps.get("HydraulicSystem")),
        "vibration_index_x": safe_float(vib.get("Vibration_Index_X")),
        "vibration_index_y": safe_float(vib.get("Vibration_Index_Y")),
        "vibration_index_z": safe_float(vib.get("Vibration_Index_Z")),
        "implement_id": implement.get("Implement_ID"),
        "section1_status": section.get("Section1"),
        "section2_status": section.get("Section2"),
        "section3_status": section.get("Section3"),
        "flow_rate_l_min": safe_float(implement.get("Flow_Rate_L_min")),
        "seed_rate_seeds_per_sq_m": safe_float(implement.get("Seed_Rate_seeds_per_sq_m")),
        "yield_moisture_content_percentage": safe_float(yield_data.get("Yield_Moisture_Content_Percentage")),
        "yield_mass_flow_rate_kg_s": safe_float(yield_data.get("Yield_Mass_Flow_Rate_kg_s")),
        "yield_rate_per_hectare_kg_ha": safe_float(yield_data.get("Yield_Rate_Per_Hectare_kg_ha")),
        "soil_moisture_percentage": safe_float(soil.get("Soil_Moisture_Percentage")),
        "soil_temperature_c": safe_float(soil.get("Soil_Temperature_C")),
        "nitrogen_ppm": safe_int(nutrients.get("Nitrogen_ppm")),
        "phosphorus_ppm": safe_int(nutrients.get("Phosphorus_ppm")),
        "potassium_ppm": safe_int(nutrients.get("Potassium_ppm"))
    }

#Enriquecer datos
def add_derived_metrics(record):
    result = record.copy()

    #Eficencia del combustible (km / L)
    speed = record.get("speed_km_h")
    fuel_rate = record.get("fuel_consumption_rate_l_hr")
    if speed is not None and fuel_rate not in (None, 0):
        result["fuel_efficiency_km_per_l"] = speed / fuel_rate
    else:
        result["fuel_efficiency_km_per_l"] = None

    #Índice de estrés del motor = (RPM * Load%) / 100
    rpm = record.get("engine_rpm")
    load = record.get("engine_load_percentage")
    if rpm is not None and load is not None:
        result["engine_stress_index"] = (rpm * load) / 100
    else:
        result["engine_stress_index"] = None

    #Índice de salud del sistema hidraulico = pressure / (temp + 1)
    pressure = record.get("hydraulic_pressure_psi")
    temp = record.get("hydraulic_fluid_temp_c")
    if pressure is not None and temp is not None:
        result["hydraulic_health_index"] = pressure / (temp + 1)
    else:
        result["hydraulic_health_index"] = None

    #Magnitud de la vibración
    vx = record.get("vibration_index_x")
    vy = record.get("vibration_index_y")
    vz = record.get("vibration_index_z")
    if None not in (vx, vy, vz):
        result["vibration_magnitude"] = (vx**2 + vy**2 + vz**2) ** 0.5 #raíz de la suma de cuadrados
    else:
        result["vibration_magnitude"] = None

    #Flow por sección activa
    flow_rate = record.get("flow_rate_l_min")
    active_sections = sum([
        bool(record.get("section1_status")),
        bool(record.get("section2_status")),
        bool(record.get("section3_status"))
    ])
    #calcular flow por sección solo si hay secciones activas
    if flow_rate is not None and active_sections > 0:
        result["flow_per_section_l_min"] = flow_rate / active_sections
    else:
        result["flow_per_section_l_min"] = None

    #Anomalías marcadas
    flags = []

    engine_temp = record.get("engine_temp_c")
    #si la temperatura del motor es mayor a 100 entonces marcar que está alta
    if engine_temp is not None and engine_temp > 100:
        flags.append("overheat")

    vibration_mag = result.get("vibration_magnitude")
    #si la magnitud de la vibración es mayor a 0,7 entonces marcar que está alta
    if vibration_mag is not None and vibration_mag > 0.7:
        flags.append("high_vibration")

    result["status_flags"] = flags if flags else None

    return result
