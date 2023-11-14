# Data Analysis Task Using PyDeequ

## Objective

Develop a data analysis task using the `pydeequ` library in Python to analyze data on available shoe products. The data is supplied from an external source system. The analysis should be done using `AnalyzerContext`, and the report should be saved in a folder as specified by `report_path`.

## Dataset Structure

The dataset consists of the following columns:

- `id`: Unique identifier of the object
- `vendor_code`: Unique identifier of the item (product)
- `name`: Name of the model
- `type`: Type of shoe model
- `label`: Brand
- `price`: Price in USD
- `discount`: Discount in percentage [0-100]
- `available_count`: Quantity available in stock
- `preorder_count`: Quantity of pre-orders

## Analysis Conditions

The analysis should cover the following conditions:

1. **Size of the Dataset**: Determine the total number of records in the dataset.
2. **Completeness of All Columns (x9)**: Check if all columns (`id`, `vendor_code`, `name`, `type`, `label`, `price`, `discount`, `available_count`, `preorder_count`) are complete without missing values.
3. **Uniqueness of `id`**: Ensure that the `id` column contains only unique values.
4. **Records with Negative Discount**: Identify if there are any records with a discount less than 0%.
5. **Records with Excessive Discount**: Check for records with a discount greater than 100%.
6. **Records with Negative Stock Quantity**: Find records where the available quantity in stock (`available_count`) is less than 0.
7. **Records with Negative Pre-order Quantity**: Identify records with a pre-order quantity (`preorder_count`) less than 0.

The report should include the results for all 15 conditions.
