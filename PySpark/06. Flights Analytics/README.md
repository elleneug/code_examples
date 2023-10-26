✈️
# Flights

This application is an example of data processing using Apache Spark (PySpark, SparkDataFrames).

## Data Description

For the demonstration, we will use real data about performed flights from the ([2015 Flight Delays and Cancellations](https://www.kaggle.com/datasets/usdot/flight-delays)) dataset. Aviation communication is one of the most complex mechanisms that require smooth organization. It is actively used for transporting both passengers and cargo, and all incidents that arise must be resolved instantly - so to speak, "on the fly." The average number of planes in the air at any given moment usually fluctuates around 11-12 thousand. This is a huge amount of data.


For simplicity, we will work with 50% of the main dataset and have prepared the following datasets for you that will be used in the practice:

- [airlines.parquet](https://yadi.sk/d/SZGXSHIk5MyYpQ) - file containing airline data
- [airports.parquet ](https://yadi.sk/d/_K_shixZh-2mRQ)- file containing airport data
- [flights.parquet](https://disk.yandex.ru/d/YxILVxCaZA0iXQ) - file containing flight data

**Airlines**

| **Column** | **Type** |  **Description**   |
|:----------:|:--------:|:------------------:|
| IATA_CODE  |  String  | Airline Identifier |
|  AIRLINE   |  String  |    Airline Name    |

**Airports**

| **Column** | **Type** |    **Description**    |
|:----------:|:--------:|:---------------------:|
| IATA_CODE  |  String  |  Airport Identifier   |
|  AIRPORT   |  String  |     Airport Name      |
|    CITY    |  String  |         City          |
|   STATE    |  String  |     State/County      |
|  COUNTRY   |  String  |        Country        |
|  LATITUDE  |  Float   | Latitude Coordinates  |
| LONGITUDE  |  Float   | Longitude Coordinates |

**Flights**

|     **Column**      | **Type** |                                        **Description**                                        |
|---------------------|-----------|----------------------------------------------------------------------------------|
|         YEAR        |  Integer  |                                Year of the flight                                |
|        MONTH        |  Integer  |                                Month of the flight                               |
|         DAY         |  Integer  |                                 Day of the flight                                |
|     DAY_OF_WEEK     |  Integer  |                         Day of the week [1-7] = [Mon-Sun]                        |
|       AIRLINE       |   String  |                                   Airline Code                                   |
|    FLIGHT_NUMBER    |   String  |                           Flight Identifier (simple ID)                          |
|     TAIL_NUMBER     |   String  |                                   Flight Number                                  |
|    ORIGIN_AIRPORT   |   String  |                              Departure Airport Code                              |
| DESTINATION_AIRPORT |   String  |                             Destination Airport Code                             |
| SCHEDULED_DEPARTURE |  Integer  |                             Scheduled departure time                             |
|    DEPARTURE_TIME   |  Integer  |                   Actual departure time (WHEEL_OFF - TAXI_OUT)                   |
|   DEPARTURE_DELAY   |  Integer  |                              Overall departure delay                             |
|       TAXI_OUT      |  Integer  |        Time between departing the gate at departure airport and taking off       |
|      WHEELS_OFF     |  Integer  |                 Time when the airplane's wheels leave the ground                 |
|    SCHEDULED_TIME   |  Integer  |                         Scheduled duration of the flight                         |
|     ELAPSED_TIME    |  Integer  |                           AIR_TIME + TAXI_IN + TAXI_OUT                          |
|       AIR_TIME      |  Integer  |            Time in the air. Interval between WHEELS_OFF and WHEELS_ON            |
|       DISTANCE      |  Integer  |                           Distance between two airports                          |
|      WHEELS_ON      |  Integer  |                 Time when the airplane's wheels touch the ground                 |
|       TAXI_IN       |  Integer  |     Time between landing and arriving at the gate at the destination airport     |
|  SCHEDULED_ARRIVAL  |  Integer  |                              Scheduled arrival time                              |
|     ARRIVAL_TIME    |  Integer  | Time when the airplane actually arrives at the destination airport (at the gate) |
|    ARRIVAL_DELAY    |  Integer  |               Arrival delay time (ARRIVAL_TIME - SCHEDULED_ARRIVAL)              |
|       DIVERTED      |  Integer  |                 Flag indicating whether the flight diverted (0/1)                |
|      CANCELLED      |  Integer  |               Flag indicating whether the flight was canceled (0/1)              |
| CANCELLATION_REASON |   String  |      Reason for flight cancellation: A - Airline/Carrier; B - Weather; etc.      |
|   AIR_SYSTEM_DELAY  |  Integer  |                        Delay time due to air system issues                       |
|    SECURITY_DELAY   |  Integer  |                         Delay time due to security issues                        |
|    AIRLINE_DELAY    |  Integer  |                         Delay time due to airline issues                         |
| LATE_AIRCRAFT_DELAY |  Integer  |                          Delay time due to late aircraft                         |
|    WEATHER_DELAY    |  Integer  |                       Delay time due to weather conditions                       |

### PySparkJob1

Let's create a pivot table showing the top 10 flights by flight code (TAIL_NUMBER) and the number of departures over all time. We will filter out records without a flight code.

Example output:

| **TAIL_NUMBER** | **count** |
|-----------------|-----------|
| N480HA          | 1763      |
| N484HA          | 1654      |
| N481HA          | 1434      |

We will save the pivot table in the Parquet format.


### PySparkJob2

Let's find the top 10 air routes (ORIGIN_AIRPORT, DESTINATION_AIRPORT) based on the highest number of flights, and calculate the average flight time (AIR_TIME).

Fields:

| **Column**          | **Description**                        |
|---------------------|----------------------------------------|
| ORIGIN_AIRPORT      | Departure airport                        |
| DESTINATION_AIRPORT | Destination airport                     |
| tail_count          | Number of flights on the route (TAIL_NUMBER) |
| avg_air_time        | Average time in the air for the route       |

### PySparkJob3


Identification of the list of airports with the most severe departure delay issues. For this, it is necessary to calculate the average, minimum, and maximum delay time and select only those airports where the maximum delay (DEPARTURE_DELAY) is 1000 seconds or more. Additionally, calculate the correlation between the delay time and the day of the week (DAY_OF_WEEK).

### Required Fields:

| Field                    | Description                                                       |
|--------------------------|-------------------------------------------------------------------|
| ORIGIN_AIRPORT           | Departure airport code                                            |
| avg_delay                | Average delay time for the airport                                |
| min_delay                | Minimum delay time for the airport                                |
| max_delay                | Maximum delay time for the airport                                |
| corr_delay2day_of_week   | Correlation between delay time and the day of the week            |


**Task Launch Parameters:**

- `flights_path` - path to the data file

- `result_path` - path where the result will be saved


## PySparkJob4

For a dashboard displaying completed flights, a table based on our data needs to be compiled. No additional data filtering is required.

### Required Fields:

| Field                      | Description                                                        |
|----------------------------|--------------------------------------------------------------------|
| AIRLINE_NAME               | Airline name (airlines.AIRLINE)                                    |
| TAIL_NUMBER                | Flight number (flights.TAIL_NUMBER)                                |
| ORIGIN_COUNTRY             | Departure country (airports.COUNTRY)                               |
| ORIGIN_AIRPORT_NAME        | Full name of the departure airport (airports.AIRPORT)              |
| ORIGIN_LATITUDE            | Latitude of the departure airport (airports.LATITUDE)              |
| ORIGIN_LONGITUDE           | Longitude of the departure airport (airports.LONGITUDE)            |
| DESTINATION_COUNTRY        | Arrival country (airports.COUNTRY)                                 |
| DESTINATION_AIRPORT_NAME   | Full name of the arrival airport (airports.AIRPORT)                |
| DESTINATION_LATITUDE       | Latitude of the arrival airport (airports.LATITUDE)                |
| DESTINATION_LONGITUDE      | Longitude of the arrival airport (airports.LONGITUDE)              |


**Task Launch Parameters:**

- `flights_path` - path to the file with flight data

- `airlines_path` - path to the file with airline data

- `airports_path` - path to the file with airport data

- `result_path` - path where the result will be saved

## PySparkJob5

The analytics department is interested in statistics regarding company issues. There's a task to construct a pivot table about all airlines containing the following data:

### Columns:

| Column                  | Description                                                                          |
|-------------------------|--------------------------------------------------------------------------------------|
| AIRLINE_NAME            | Airline name [airlines.AIRLINE]                                                       |
| correct_count           | Number of completed flights                                                           |
| diverted_count          | Number of flights completed with a delay                                              |
| cancelled_count         | Number of cancelled flights                                                           |
| avg_distance            | Average flight distance                                                              |
| avg_air_time            | Average time in the air                                                              |
| airline_issue_count     | Number of delays due to aircraft issues [CANCELLATION_REASON]                         |
| weather_issue_count     | Number of delays due to weather conditions [CANCELLATION_REASON]                      |
| nas_issue_count         | Number of delays due to NAS issues [CANCELLATION_REASON]                              |
| security_issue_count    | Number of delays due to security services [CANCELLATION_REASON]                       |


**Task Launch Parameters:**

- `flights_path` - path to the file with flight data

- `airlines_path` - path to the file with airline data

- `result_path` - path where the result will be saved
