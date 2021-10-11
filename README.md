# nike-assignment

## Spark Programming Task

The aim of this exercise is to create bunch of simple JSON extract files from various csv sources based on the conditions (weekly aggregator).

Source files sales.csv, calendar.csv, store.csv and product.csv
Destination files format consumption.json

**Sales data** contains transactional data with the following columns netSales, salesUnits, dateId, productId and storeId. These fields need to be joined with other fields from other csv’s to get the weekly aggregated value for each combination.

**Calendar data** contains dateId, week number and year. (Criteria: sales.dateId should be equal to calendar.dateId to get weekNumber and year) 

**Product data** contains productId vs hierarchy. (division, gender, category) (Criteria : sales.productId = product.productId)

**Store data** contains store vs channel information. (Criteria:sales.storeId = store.storeId)

Output should be bunch of json files which is having the weekly aggregated values of net_sales and sales_units. The json files will have unique key. (Refer: consumption.json) 

**Assumptions**
1. I am assuming that i am running the task only for one year.

**TO RUN THE SPARK**

- Spark programming task is running locally. Provided data in calendar.csv didn't match the assignment description( week number ) therefore function to calculate week of year was created.
- I created one extra line in sales.csv to show that output is working for more data
- To RUN spark assignment localy: 
"Usage: Nike assignment data files <file> <calendar_data_file> <product_data_file> <sales_data_file> <store_data_file> "


## Covering note
TO DO: 
- Pivot a Spark DataFrame
- Unit testing
- Load the source and destination files to the s3 buckets
- AWS EMR Deployment

