# Python script to test scalability of Task 1

import time
import requests
import matplotlib.pyplot as plt

# Task 1 – LeedsWeatherSimulator URL
FUNCTION_URL = (
    "https://sinjini-comp3211-func-bag6ajetduaehcgr.switzerlandnorth-01.azurewebsites.net"
    "/api/LeedsWeatherSimulator"
)

# Different numbers of records per sensor to test scalability - x axis
record_counts = [10, 50, 100, 200, 500, 1000]

# List to store response times
response_times = []

# for loop to go through every number of records in record_counts
for n in record_counts:
    start_time = time.perf_counter()

    # Make the HTTP request
    resp = requests.get(f"{FUNCTION_URL}?number_of_records={n}")

    end_time = time.perf_counter()
    duration_ms = (end_time - start_time) * 1000.0

    response_times.append(duration_ms)

    # error validation
    if resp.status_code == 200:
        print(f"{n} records/sensor: {duration_ms:.2f} ms")
    else:
        print(f"{n} records/sensor: Error {resp.status_code}")

# Plot the scalability graph for Task 1
plt.figure()
plt.plot(record_counts, response_times, marker="o")
plt.xlabel("Number of records per sensor")
plt.ylabel("Response time (ms)")
plt.title("Task 1 - Scalability of LeedsWeatherSimulator")
plt.grid(True)
plt.tight_layout()
plt.savefig("task1_scalability.png")
plt.show()