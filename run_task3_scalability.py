# Python script to test scalability of Task 3

import matplotlib.pyplot as plt

# Invocation numbers 1 to 12 (this is basically taken from the invocations tab, the first 12 rows)
invocations = list(range(1, 13))

# Durations from your Azure screenshot (oldest → newest)
durations_ms = [15, 58, 50, 42, 22, 14, 55, 16, 68, 16, 27, 26]

plt.figure()
plt.plot(invocations, durations_ms, marker="o")
plt.xlabel("Task3_DataTimer Invocation Number")
plt.ylabel("Execution Time (ms)")
plt.title("Task 3 - Runtime of Timer-Triggered Function Over Consecutive Invocations")
plt.grid(True)
plt.tight_layout()
plt.savefig("task3_scalability.png")
plt.show()