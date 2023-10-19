# Task #1: Bank Credit Scoring

Credit scoring is a system used by banks to evaluate clients, based on statistical methods. Typically, it's an algorithm into which the potential borrower's data is entered. The result is then generated – determining whether to grant credit to the person.

## Data Structure:

| Field                        | Description                                                       |
|------------------------------|-------------------------------------------------------------------|
| client_id                    | Unique client identifier                                          |
| age                          | Client's age at the time of review                                |
| sex                          | Client's gender                                                   |
| married                      | Marital status                                                    |
| salary                       | Client's official and confirmed salary                            |
| successfully_credit_completed| Number of successfully paid off credits                           |
| credit_completed_amount      | Total amount of paid off credits                                  |
| active_credits               | Number of active credits                                          |
| active_credits_amount        | Total amount of active credits                                    |
| credit_amount                | Credit amount                                                     |
| is_credit_closed             | Indicator showing if the credit issued to the client was successfully closed (paid to the bank) |

Implement the missing logic for training the model to assess the reliability of bank customers. Use the accuracy metric for evaluation.

**Note:** Boolean data types should be converted to numeric format. For instance:
```python
df.withColumn("is_married", df.married.cast(IntegerType()))
```


# Task #2: Bank Credit Evaluation

Now, it's required to implement a model to predict the maximum credit amount that the bank can offer to a client, based on the data about him/her.

## Data Structure:

| Field                        | Description                                                       |
|------------------------------|-------------------------------------------------------------------|
| client_id                    | Unique client identifier                                          |
| age                          | Client's age at the time of review                                |
| sex                          | Client's gender                                                   |
| married                      | Marital status                                                    |
| salary                       | Client's official and confirmed salary                            |
| successfully_credit_completed| Number of successfully paid off credits                           |
| credit_completed_amount      | Total amount of paid off credits                                  |
| active_credits               | Number of active credits                                          |
| active_credits_amount        | Total amount of active credits                                    |
| credit_amount                | Credit amount                                                     |

Implement the missing logic for training the model to estimate the maximum credit amount for a bank client. Use the RMSE metric for evaluation.
