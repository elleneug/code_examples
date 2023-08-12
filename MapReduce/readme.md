Preparation:

- Create an S3 bucket in Object Storage (documentation).
- Copy the yellow taxi data for the year 2020 into the created S3 bucket.

Task:
Write a map-reduce application that uses the copied data on Object Storage and calculates a report for each month of the year 2020 in the following format:

----------------------------------------------
| Month	| Payment type	| Tips average amount |
|------------------------------------------|
|2020-01	| Credit card	| 999.99 |
-----------------------------------------------



Report Requirements:

1. Number of files: 1
2. Format: csv
3. Sorting: Not required
4. Month is determined based on the tpep_pickup_datetime field
5. Payment type is determined based on the payment_type field
6.Tips average amount is calculated based on the tip_amount field
7. Note: Invalid data will be present in the dataset (incorrect year, empty payment type) - these data points need to be filtered out.

Mapping of payment types:
mapping = {
1: 'Credit card',
2: 'Cash',
3: 'No charge',
4: 'Dispute',
5: 'Unknown',
6: 'Voided trip'
}
