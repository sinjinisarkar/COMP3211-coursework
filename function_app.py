import azure.functions as func
import logging
import json
import random
import time
from datetime import datetime
from azure.functions.decorators.core import DataType

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

def generate_sql_rows(sensor_count: int) -> func.SqlRowList:
    """
    Create a list of SqlRow objects ready to be written into dbo.SensorData.
    Uses the existing Sensor class to generate readings that match the DB schema.
    """
    rows = []
    for sensor_id in range(1, sensor_count + 1):
        reading = Sensor(sensor_id).generate_reading()
        row = func.SqlRow(
            SensorId=reading["sensor_id"],
            Temperature=reading["temperature_c"],
            WindSpeed=reading["wind_mph"],
            RelativeHumidity=reading["humidity_percent"],
            CO2=reading["co2_ppm"]
        )
        rows.append(row)
    return rows

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

# --- Task 3a: Timer-triggered data function (writes to SQL) ---
@app.generic_output_binding(
    arg_name="rows",
    type="sql",
    CommandText="dbo.SensorData",              # matches your SQL table
    ConnectionStringSetting="SqlConnectionString",
    data_type=DataType.STRING
)
@app.function_name(name="Task3_DataTimer")
@app.timer_trigger(
    schedule="0 */5 * * * *",   # every 5 minutes
    arg_name="myTimer",
    run_on_startup=True,        # run once immediately on host start (useful for testing)
    use_monitor=True
)
def task3_data_timer(myTimer: func.TimerRequest,
                     rows: func.Out[func.SqlRowList]) -> None:
    """
    Task 3a - Data function.
    Timer-triggered Azure Function that writes ONE reading per sensor
    into the SensorData SQL table using an output binding.
    """

    logging.info("Task3_DataTimer triggered.")

    sensor_count = 20  # 20 virtual sensors in Leeds
    sql_rows = generate_sql_rows(sensor_count)

    # Write all rows to SQL in one operation
    rows.set(sql_rows)

    logging.info("Task3_DataTimer inserted %d rows into SensorData.", len(sql_rows))

# --- Task 3b: SQL-triggered statistics function ---
# This trigger fires whenever SensorData changes
@app.generic_trigger(
    arg_name="changes",
    type="sqlTrigger",
    TableName="dbo.SensorData",
    ConnectionStringSetting="SqlConnectionString",
    data_type=DataType.STRING
)
# Input binding to read all rows from SensorData
@app.generic_input_binding(
    arg_name="all_rows",
    type="sql",
    CommandText="SELECT * FROM dbo.SensorData",
    CommandType="Text",
    ConnectionStringSetting="SqlConnectionString",
    data_type=DataType.STRING
)
@app.function_name(name="Task3_StatsSqlTrigger")
def task3_stats_sql_trigger(changes,
                            all_rows: func.SqlRowList) -> None:
    """
    Task 3(b) - Statistics function triggered by SQL.
    When SensorData changes, this reads ALL rows from the table
    and logs min / max / average for each sensor, same as Task 2.
    """

    logging.info("Task3_StatsSqlTrigger fired due to change in SensorData.")

    # Convert each SqlRow into a normal Python dict
    records = [json.loads(row.to_json()) for row in all_rows]

    if not records:
        logging.info("No records found in SensorData table.")
        return

    # Helper to compute average
    def average(values):
        return sum(values) / len(values) if values else 0

    # Group rows by SensorId
    grouped = {}
    for r in records:
        sid = r["SensorId"]
        grouped.setdefault(sid, []).append(r)

    # Compute stats per sensor (same structure as Task 2)
    stats_per_sensor = {}
    for sid, values in grouped.items():
        temps = [v["Temperature"] for v in values]
        winds = [v["WindSpeed"] for v in values]
        hums  = [v["RelativeHumidity"] for v in values]
        co2   = [v["CO2"] for v in values]

        stats_per_sensor[f"Sensor_{sid}"] = {
            "temperature": {
                "min": min(temps),
                "max": max(temps),
                "average": round(average(temps), 2),
            },
            "wind_speed": {
                "min": min(winds),
                "max": max(winds),
                "average": round(average(winds), 2),
            },
            "humidity": {
                "min": min(hums),
                "max": max(hums),
                "average": round(average(hums), 2),
            },
            "co2": {
                "min": min(co2),
                "max": max(co2),
                "average": round(average(co2), 2),
            },
        }

    # Because this is a SQL trigger (not HTTP), we log the JSON
    logging.info("Task3_StatsSqlTrigger statistics:\n%s",
                 json.dumps(stats_per_sensor, indent=2))