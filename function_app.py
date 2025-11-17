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

def generate_sql_rows_for_one_cycle(sensor_count: int) -> func.SqlRowList:
    """
    Generate exactly ONE reading per sensor, returned as SqlRowList.
    Used by Task 3 (timer) to insert 1 row per sensor.
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


def generate_sql_rows_for_task1(number_of_records: int,
                                sensor_count: int = 20) -> tuple[list[dict], func.SqlRowList]:
    """
    Generate 'number_of_records' readings per sensor (so sensor_count * number_of_records rows).
    Returns BOTH:
      - readings_json: list of dicts for the HTTP response
      - rows_sql: SqlRowList for insertion into dbo.SensorData
    """
    readings_json: list[dict] = []
    rows_sql: list[func.SqlRow] = []

    for _ in range(number_of_records):
        for sensor_id in range(1, sensor_count + 1):
            reading = Sensor(sensor_id).generate_reading()
            readings_json.append(reading)

            row = func.SqlRow(
                SensorId=reading["sensor_id"],
                Temperature=reading["temperature_c"],
                WindSpeed=reading["wind_mph"],
                RelativeHumidity=reading["humidity_percent"],
                CO2=reading["co2_ppm"]
            )
            rows_sql.append(row)

    return readings_json, rows_sql

# --- Task 1: Simulated Data ---
@app.generic_output_binding(
    arg_name="rows_out",
    type="sql",
    CommandText="dbo.SensorData",
    ConnectionStringSetting="SqlConnectionString",
    data_type=DataType.STRING
)
@app.function_name(name="LeedsWeatherSimulator")
@app.route(route="LeedsWeatherSimulator")
def leeds_weather_simulator(req: func.HttpRequest,
                            rows_out: func.Out[func.SqlRowList]) -> func.HttpResponse:
    """
    Task 1 - Simulated Data Function.
    Simulates data from 20 sensors in Leeds.
    Uses ?number_of_records=<N> to control how many readings per sensor are generated.
    Writes all generated records into dbo.SensorData and returns timing + readings as JSON.
    """

    logging.info("LeedsWeatherSimulator triggered.")

    try:
        # We always simulate 20 sensors (as per coursework brief)
        sensor_count = 20

        # Read the 'number_of_records' query parameter
        num_param = req.params.get("number_of_records")

        if not num_param:
            return func.HttpResponse(
                "Please provide 'number_of_records' in the query string, e.g. "
                "/api/LeedsWeatherSimulator?number_of_records=10",
                status_code=400
            )

        if (not num_param.isdigit()) or int(num_param) <= 0:
            return func.HttpResponse(
                "Invalid 'number_of_records' parameter. Please provide a positive integer.",
                status_code=400
            )

        number_of_records = int(num_param)

        # Measure time to generate and store the data
        start = time.perf_counter()
        readings_json, rows_sql = generate_sql_rows_for_task1(
            number_of_records=number_of_records,
            sensor_count=sensor_count
        )

        # Write all rows to SQL in one operation
        rows_out.set(rows_sql)

        end = time.perf_counter()
        duration_ms = (end - start) * 1000.0

        total_rows = len(rows_sql)  # should be sensor_count * number_of_records

        result = {
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "city": "Leeds",
            "sensor_count": sensor_count,
            "number_of_records_per_sensor": number_of_records,
            "total_rows_inserted": total_rows,
            "time_ms": round(duration_ms, 3),
            "readings": readings_json,
        }

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
@app.generic_input_binding(
    arg_name="sensor_rows",
    type="sql",
    CommandText="SELECT * FROM dbo.SensorData",
    CommandType="Text",
    ConnectionStringSetting="SqlConnectionString",
    data_type=DataType.STRING
)
@app.function_name(name="LeedsWeatherStats")
@app.route(route="LeedsWeatherStats", methods=["GET"])
def leeds_weather_stats(req: func.HttpRequest,
                        sensor_rows: func.SqlRowList) -> func.HttpResponse:
    """
    Task 2 - Statistics Function (HTTP).
    Reads ALL rows from dbo.SensorData via SQL input binding
    and returns min / max / average per sensor as JSON.
    """

    logging.info("LeedsWeatherStats (DB-based) function triggered.")

    # Convert SqlRow objects to Python dicts
    records = [json.loads(row.to_json()) for row in sensor_rows]

    if not records:
        return func.HttpResponse(
            "No data found in SensorData table.",
            status_code=200
        )

    # Helper to compute average
    def average(values):
        return sum(values) / len(values) if values else 0

    # Group by SensorId
    grouped = {}
    for r in records:
        sid = r["SensorId"]
        grouped.setdefault(sid, []).append(r)

    # Compute stats per sensor
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

    return func.HttpResponse(
        json.dumps(stats_per_sensor, indent=2),
        mimetype="application/json",
        status_code=200,
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
    sql_rows = generate_sql_rows_for_one_cycle(sensor_count)

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
@app.function_name(name="Task3_StatsSqlTriggerV2")   # NEW NAME
def task3_stats_sql_trigger_v2(changes,              # NEW FUNCTION NAME
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
    logging.info("Task3_StatsSqlTriggerV2 statistics:\n%s",
                 json.dumps(stats_per_sensor, indent=2))




