# Distributed Systems Coursework 2
## Serverless Architectures with Azure Functions
By Sinjini Sarkar (sc23ss2)

### Task 1
This task implements a simulated data function that generates weather readings from 20 virtual sensors around Leeds.
The function is HTTP-triggered and takes a query parameter (number_of_records) to control how many readings per sensor are produced.
All generated rows are written into the Azure SQL database using an output binding.

### Task 2
This task implements a statistics function that reads all rows from the Azure SQL database and computes minimum, maximum and average values for each sensor.
The function is HTTP-triggered and uses a SQL input binding to fetch the data.

### Task 3
This task combines both earlier tasks into a more realistic serverless workflow.
A timer-triggered function runs every 5 minutes and inserts one new reading per sensor into the database.
A second function is triggered automatically when the database table changes and recalculates the statistics.