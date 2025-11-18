import time
import requests
import matplotlib.pyplot as plt

# URLs
FUNCTION1_URL = "https://sinjini-comp3211-func-bag6ajetduaehcgr.switzerlandnorth-01.azurewebsites.net/api/LeedsWeatherSimulator"
FUNCTION2_URL = "https://sinjini-comp3211-func-bag6ajetduaehcgr.switzerlandnorth-01.azurewebsites.net/api/LeedsWeatherStats"

# Task 1 parameter
PARAM = "number_of_records"
SENSORS = 20

# How many new records (per sensor) to insert each round
batches = [10, 50, 100, 200, 500, 1000]

total = 0
x = []
y = []

for b in batches:
    # Insert more data via Task 1
    r1 = requests.get(FUNCTION1_URL, params={PARAM: b})
    if r1.status_code != 200:
        print(f"Task 1 error ({r1.status_code}) for batch {b}")
        continue

    total += b * SENSORS

    # Time Task 2
    t0 = time.perf_counter()
    r2 = requests.get(FUNCTION2_URL)
    t1 = time.perf_counter()

    dt = (t1 - t0) * 1000
    x.append(total)
    y.append(dt)

    print(f"{total} rows: {dt:.2f} ms")

# Plot
plt.plot(x, y, marker="o")
plt.xlabel("Total rows in SensorData")
plt.ylabel("Response time (ms)")
plt.title("Task 2 - Scalability")
plt.grid(True)
plt.tight_layout()
plt.savefig("task2_scalability.png")
plt.show()