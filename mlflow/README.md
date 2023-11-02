# Automation of Insurance Company Processes

## Objective

The task at hand is to classify drivers by predicting the probability of a driver having an accident in the upcoming year, for which the driver plans to purchase insurance. This prediction is based on statistics collected from clients.

## Applications:

1) `PySparkFit.py` - Model training process
2) `PySparkPredict.py` - Model Prediction process

## Dataset Structure:

| Field              | Description                                               |
|--------------------|-----------------------------------------------------------|
| `driver_id`        | Unique driver identifier                                  |
| `age`              | Driver's age at the time of analysis                      |
| `sex`              | Driver's gender                                           |
| `car_class`        | Class of the driver's car                                 |
| `driving_experience`| Driving experience                                        |
| `speeding_penalties`| Number of speeding penalties within a year               |
| `parking_penalties` | Number of parking penalties within a year                |
| `total_car_accident`| Number of accidents throughout driving experience        |
| `has_car_accident`  | Identifier for accident occurrence in the current year (target feature [0/1])|

## Metrics:

- f1
- weightedPrecision
- weightedRecall
- accuracy

## Parameters:

- `input_columns` - List of original dataset columns (Example: ['col1', 'col2'])
- `maxDepth` - Parameter for the maximum depth of the trained model
- `maxIter` - Parameter for the maximum number of training iterations
- `maxBins` - Parameter for the maximum number of branches
- `target` - Target variable for predictions
- `features` - List of columns used for vectorization (Example: ['col1', 'col2'])
- `stage_0` - Type of the first stage transformer in the pipeline (`obj.__class__.__name__`)
- `stage_1` - Type of the second stage transformer in the pipeline (`obj.__class__.__name__`)
- `stage_2` - Type of the third stage transformer in the pipeline (`obj.__class__.__name__`)
- `stage_3` - Type of the fourth stage transformer in the pipeline (`obj.__class__.__name__`)

The model (`registered_model_name`) and its location (`artifact_path`) should also be logged.
