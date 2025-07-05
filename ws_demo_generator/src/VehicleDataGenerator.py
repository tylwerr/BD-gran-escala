import random
import datetime
import json

class VehicleDataGenerator:
    """
    A class to generate random vehicle and implement data records.
    Allows 'Implement' and 'Diagnostic_Data' to be null based on probabilities.
    """
    def __init__(self, num_records):
        """
        Initializes the VehicleDataGenerator.

        Args:
            num_records (int): The total number of data records to generate.
        """
        if not isinstance(num_records, int):
            raise ValueError("num_records must be a non-negative integer.")
        self.num_records = num_records
        self._vehicle_id_counter = 0
        self._implement_id_counter = 0
        self._generated_count = 0

    def __iter__(self):
        """
        Makes the class iterable, allowing it to be used directly in a for loop.
        """
        return self

    def __next__(self):
        """
        Generates the next random vehicle data record.
        """
        if self.num_records>0 and self._generated_count >= self.num_records:
            raise StopIteration

        self._vehicle_id_counter += 1
        self._implement_id_counter += 1
        vehicle_id = f"AGR-{self._vehicle_id_counter:03d}"

        # Use the current time at the moment of generation.
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')

        latitude = round(random.uniform(-33.46, -33.44), 4)
        longitude = round(random.uniform(-70.68, -70.66), 4)
        engine_rpm = random.randint(1500, 2500)
        engine_load_percentage = random.randint(30, 95)
        fuel_level_percentage = random.randint(5, 100)
        fuel_consumption_rate_l_hr = round(random.uniform(5.0, 20.0), 1)
        speed_km_h = round(random.uniform(0.0, 60.0), 1)
        odometer_km = round(random.uniform(10000.0, 20000.0), 1)
        operating_hours = round(random.uniform(1500.0, 3000.0), 1)
        hydraulic_pressure_psi = random.randint(2000, 3000)
        hydraulic_fluid_temp_c = random.randint(70, 100)
        engine_coolant_temp_c = random.randint(80, 105)
        battery_voltage_v = round(random.uniform(23.5, 25.0), 1)
        gear_engaged = random.choice(["Neutral", "1st", "2nd", "3rd", "4th", "Reverse"])
        accelerator_pedal_position_percentage = random.randint(0, 100)
        brake_pedal_position_percentage = random.randint(0, 100) if accelerator_pedal_position_percentage < 10 else 0

        # --- Diagnostic Data (can be None) ---
        diagnostic_data = None
        if random.random() > 0.2:  # 80% chance of having diagnostic data
            error_code = random.choice([None, "P0420", "U0100", "C1234"])
            check_engine_light = random.choice(["On", "Off"])
            low_oil_pressure = random.choice(["On", "Off"])
            comp_temp_engine = random.randint(85, 100)
            comp_temp_transmission = random.randint(75, 95)
            comp_temp_hydraulic_system = random.randint(80, 98)
            vibration_x = round(random.uniform(0.3, 0.7), 2)
            vibration_y = round(random.uniform(0.3, 0.7), 2)
            vibration_z = round(random.uniform(0.3, 0.7), 2)

            diagnostic_data = {
                "Error_Code": error_code,
                "Warning_Lights": {
                    "CheckEngineLight": check_engine_light,
                    "LowOilPressure": low_oil_pressure
                },
                "Component Temperatures_C": {
                    "Engine": comp_temp_engine,
                    "Transmission": comp_temp_transmission,
                    "HydraulicSystem": comp_temp_hydraulic_system
                },
                "Vibration": {
                    "Vibration_Index_X": vibration_x,
                    "Vibration_Index_Y": vibration_y,
                    "Vibration_Index_Z": vibration_z
                }
            }

        # --- Implement Data (can be None) ---
        implement_data = None
        if random.random() > 0.3:  # 70% chance of having implement data
            implement_id = f"PLNTR-{self._implement_id_counter:03d}"
            section_control_status = {
                "Section1": random.choice(["On", "Off"]),
                "Section2": random.choice(["On", "Off"]),
                "Section3": random.choice(["On", "Off"])
            }
            flow_rate_l_min = round(random.uniform(30.0, 60.0), 1)
            seed_rate_seeds_per_sq_m = round(random.uniform(5.0, 10.0), 1)
            yield_moisture_content_percentage = round(random.uniform(10.0, 15.0), 1)
            yield_mass_flow_rate_kg_s = round(random.uniform(1.0, 3.0), 1)
            yield_rate_per_hectare_kg_ha = round(random.uniform(1000.0, 2000.0), 1)
            soil_moisture_percentage = round(random.uniform(15.0, 25.0), 1)
            soil_temperature_c = round(random.uniform(18.0, 25.0), 1)
            nitrogen_ppm = random.randint(10, 40)
            phosphorus_ppm = random.randint(5, 25)
            potassium_ppm = random.randint(20, 50)

            implement_data = {
                "Implement_ID": implement_id,
                "Section_Control_Status": section_control_status,
                "Flow_Rate_L_min": flow_rate_l_min,
                "Seed_Rate_seeds_per_sq_m": seed_rate_seeds_per_sq_m,
                "Yield_Monitor_Data": {
                    "Yield_Moisture_Content_Percentage": yield_moisture_content_percentage,
                    "Yield_Mass_Flow_Rate_kg_s": yield_mass_flow_rate_kg_s,
                    "Yield_Rate_Per_Hectare_kg_ha": yield_rate_per_hectare_kg_ha
                },
                "Soil_Sensor_Data": {
                    "Soil_Moisture_Percentage": soil_moisture_percentage,
                    "Soil_Temperature_C": soil_temperature_c,
                    "Soil_Nutrient_Levels": {
                        "Nitrogen_ppm": nitrogen_ppm,
                        "Phosphorus_ppm": phosphorus_ppm,
                        "Potassium_ppm": potassium_ppm
                    }
                }
            }

        data = {
            "Vehicle_ID": vehicle_id,
            "Timestamp": timestamp,
            "GPS_Coordinates": {
                "Latitude": latitude,
                "Longitude": longitude
            },
            "Engine_RPM": engine_rpm,
            "Engine_Load_Percentage": engine_load_percentage,
            "Fuel_Level_Percentage": fuel_level_percentage,
            "Fuel_Consumption_Rate_L_hr": fuel_consumption_rate_l_hr,
            "Speed_km_h": speed_km_h,
            "Odometer_km": odometer_km,
            "Operating_Hours": operating_hours,
            "Hydraulic_Pressure_psi": hydraulic_pressure_psi,
            "Hydraulic_Fluid_Temp_C": hydraulic_fluid_temp_c,
            "Engine_Coolant_Temp_C": engine_coolant_temp_c,
            "Battery_Voltage_V": battery_voltage_v,
            "Gear_Engaged": gear_engaged,
            "Accelerator_Pedal_Position_Percentage": accelerator_pedal_position_percentage,
            "Brake_Pedal_Position_Percentage": brake_pedal_position_percentage,
            "Diagnostic_Data": diagnostic_data,
            "Implement": implement_data
        }
        self._generated_count += 1
        return json.dumps(data)

if __name__ == "__main__":
    import json

    # Example usage: Generate 3 data points
    print("Generating 3 records:")
    generator_3_records = VehicleDataGenerator(num_records=3)
    for generated_data in generator_3_records:
        print(json.dumps(generated_data, indent=2))
        print("-" * 30)

    # Example usage: Generate 1 data point
    print("\nGenerating 1 record:")
    generator_1_record = VehicleDataGenerator(num_records=1)
    for generated_data in generator_1_record:
        print(json.dumps(generated_data, indent=2))
        print("-" * 30)

    # Example usage: Generate 0 data points (will yield nothing)
    print("\nGenerating 0 records:")
    generator_0_records = VehicleDataGenerator(num_records=0)
    for generated_data in generator_0_records:
        print(json.dumps(generated_data, indent=2))
        print("-" * 30)

    # Example of invalid input
    try:
        invalid_generator = VehicleDataGenerator(num_records=-5)
    except ValueError as e:
        print(f"\nError creating generator with invalid num_records: {e}")

    try:
        invalid_generator = VehicleDataGenerator(num_records="abc")
    except ValueError as e:
        print(f"Error creating generator with invalid num_records: {e}")