"""
RUN_SCALABILITY_TEST.PY
---------------------------------
Python script to test scalability of Azure Function (Task 1)
Name: Sinjini Sarkar (sc23ss2)
Student ID: 201695493
Module: COMP3211 Distributed Systems CWK 2
---------------------------------
"""

import time
import requests
import matplotlib.pyplot as plt

# Azure Function URL
function_url = "https://sinjini-comp3211-func-bag6ajetduaehcgr.switzerlandnorth-01.azurewebsites.net/api/LeedsWeatherSimulator"

# Different sensor counts to test scalability
sensor_counts = [10, 50, 100, 200, 500, 1000, 2000]

# List to store response times
response_times = []


for count in sensor_counts:
    start_time = time.perf_counter()

    # Make the HTTP request
    response = requests.get(f"{function_url}?count={count}")

    end_time = time.perf_counter()
    duration_ms = (end_time - start_time) * 1000.0

    # Save the time
    response_times.append(duration_ms)

    # Print progress
    if response.status_code == 200:
        print(f"{count} sensors → {duration_ms:.2f} ms")
    else:
        print(f"{count} sensors → Error {response.status_code}")

# Plotting the scalability graph
plt.figure()
plt.plot(sensor_counts, response_times, marker="o", color="purple")
plt.xlabel("Number of sensors simulated")
plt.ylabel("Response time (ms)")
plt.title("Task 1 - Scalability of LeedsWeatherSimulator")
plt.grid(True)
plt.tight_layout()
plt.savefig("task1_scalability_auto.png")
plt.show()

