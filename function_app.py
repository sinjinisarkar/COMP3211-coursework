import azure.functions as func
import logging
import json
import random
import time
from datetime import datetime

# Create Function App instance
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

"""
COMP3211 Coursework 2
Name: Sinjini Sarkar (sc23ss2)
Student ID: 201695493
Function: LeedsWeatherSimulator
"""

# Ranges (from coursework specification)
TEMP_RANGE = (5, 18) # °C
WIND_RANGE = (12, 24) # mph
HUMID_RANGE = (30, 60) # %
CO2_RANGE = (400, 1600) # ppm


class Sensor:
    """A simple model of a Leeds weather sensor."""
    def __init__(self, sensor_id: int):
        self.sensor_id = sensor_id

    def generate_reading(self) -> dict:
        """Produce one simulated reading for this sensor."""
        return {
            "sensor_id": self.sensor_id,
            "temperature_c": random.randint(*TEMP_RANGE),
            "wind_mph": random.randint(*WIND_RANGE),
            "humidity_percent": random.randint(*HUMID_RANGE),
            "co2_ppm": random.randint(*CO2_RANGE),
        }


def simulate_weather_sensors(sensor_count: int) -> list[dict]:
    """Generate readings from all sensors."""
    sensors = [Sensor(i + 1) for i in range(sensor_count)]
    return [sensor.generate_reading() for sensor in sensors]

# --- Task 1: Simulated Data ---
@app.function_name(name="LeedsWeatherSimulator")
@app.route(route="LeedsWeatherSimulator")
def leeds_weather_simulator(req: func.HttpRequest) -> func.HttpResponse:
    """
    Task 1 - Simulated Data Function.
    Simulates data from N sensors in Leeds (default 20).
    Also returns how long the simulation took in milliseconds.
    """

    logging.info("LeedsWeatherSimulator triggered.")

    try:
        # Default = 20 sensors but allow ?count= for scalability tests
        count_param = req.params.get("count")

        # Validate that 'count' is a positive integer
        if count_param:
            if not count_param.isdigit() or int(count_param) <= 0:
                return func.HttpResponse(
                    "Invalid 'count' parameter. Please provide a positive integer.",
                    status_code=400
                )
            sensor_count = int(count_param)
        else:
            sensor_count = 20

        # Measure time taken for the simulation
        start = time.perf_counter()
        readings = simulate_weather_sensors(sensor_count)
        end = time.perf_counter()
        duration_ms = (end - start) * 1000.0

        result = {
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "city": "Leeds",
            "sensor_count": sensor_count,
            "time_ms": round(duration_ms, 3),
            "readings": readings,
        }

        # Return result as JSON response
        return func.HttpResponse(
            json.dumps(result, indent=2),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"Error in LeedsWeatherSimulator: {str(e)}")
        return func.HttpResponse(
            f"An internal error occurred while generating sensor data: {str(e)}",
            status_code=500
        )

# --- Task 2: Statistics (per sensor) ---
@app.function_name(name="LeedsWeatherStats")
@app.route(route="LeedsWeatherStats")
def leeds_weather_stats(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("LeedsWeatherStats function triggered.")

    try:
        data = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON.", status_code=400)

    readings = data.get("readings", [])
    if not readings:
        return func.HttpResponse("No readings found.", status_code=400)

    # Helper to compute avg
    def average(values):
        return sum(values) / len(values) if values else 0

    # Group readings by sensor_id
    grouped = {}
    for r in readings:
        sid = r["sensor_id"]
        grouped.setdefault(sid, []).append(r)

    # Compute per sensor stats
    stats_per_sensor = {}
    for sid, values in grouped.items():
        temps = [v["temperature_c"] for v in values]
        winds = [v["wind_mph"] for v in values]
        hums  = [v["humidity_percent"] for v in values]
        co2   = [v["co2_ppm"] for v in values]

        stats_per_sensor[f"Sensor_{sid}"] = {
            "temperature": {"min": min(temps), "max": max(temps), "average": round(average(temps), 2)},
            "wind_speed": {"min": min(winds), "max": max(winds), "average": round(average(winds), 2)},
            "humidity":   {"min": min(hums),  "max": max(hums),  "average": round(average(hums), 2)},
            "co2":        {"min": min(co2),   "max": max(co2),   "average": round(average(co2), 2)}
        }

    # Return as JSON
    return func.HttpResponse(
        json.dumps(stats_per_sensor, indent=2),
        mimetype="application/json",
        status_code=200
    )