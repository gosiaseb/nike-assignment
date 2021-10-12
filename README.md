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
"Usage: Nike assignment data files <file> <calendar_data_file> <product_data_file> <sales_data_file> <store_data_file> <output_data_file>"


## Covering note
TO DO:
- Unit testing
- Load the source and destination files to the s3 buckets
- AWS EMR Deployment

1. How long did you spend on the coding test? 8 hours
2. What would you add to your solution if you had more time? Written above TO DO section
3. If you didn't spend much time on the coding test then use this as an opportunity to explain what you would add. YES
4. What was the most useful feature that was added to the latest version of your chosen language? Please include a snippet of code that shows how you've used it.
    No new feature used for this assignment, could be done as a part of TO DO.
5. How would you track down a performance issue in production? By looking at logs and spark UI for task explanation. 
6. Have you ever had to do this the above task with the terabyte of data? Not yet, but would like to.
    

