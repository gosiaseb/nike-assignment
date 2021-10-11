import datetime
import sys

from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import weekofyear, col
from pyspark.sql import functions as F


def get_date_df(calendar_df_old):
    pan = calendar_df_old.toPandas()
    list_ofindices = []
    for index, value in enumerate(pan['datecalendarday']):
        if (pan['datecalendarday'][index] == '1'):
            index_count = index
            list_ofindices.append(index_count)
    list_ofindices.append(len(pan['datecalendarday']))

    actual_CW = []
    monthsofyear = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    print(len(monthsofyear))
    for i in range(len(list_ofindices) - 1):
        for x in range(list_ofindices[i], list_ofindices[i + 1]):
            print(x)
            dets = datetime.date(int(pan['datecalendaryear'][x]), monthsofyear[i + 1], int(pan['datecalendarday'][x]))
            # calendar_week = datetime.date(int(pan['datecalendaryear'][x]), monthsofyear[i-1], int(pan['datecalendarday'][x])).isocalendar()[1]
            actual_CW.append([pan['datekey'][x], dets])
    print(actual_CW)
    date_df = spark.createDataFrame(actual_CW)
    return date_df


if __name__ == "__main__":


    spark = SparkSession.builder.master("local[*]").appName("NikeAssignment").getOrCreate()



calendar_df = spark.read.option("header", True).csv(
    "/Users/malgorzata/Documents/GitHub/nike-assignment/data/calendar.csv").hint("broadcast")
product_df = spark.read.option("header", True).csv(
    "/Users/malgorzata/Documents/GitHub/nike-assignment/data/product.csv").repartition('productId')
sales_df = spark.read.option("header", True).csv(
    "/Users/malgorzata/Documents/GitHub/nike-assignment/data/sales.csv").repartition('productId')
store_df = spark.read.option("header", True).csv(
    "/Users/malgorzata/Documents/GitHub/nike-assignment/data/store.csv").repartition('storeId')


new_date_calendar_df = get_date_df(calendar_df)
new_calendar_df = calendar_df.join(new_date_calendar_df, (calendar_df.datekey == new_date_calendar_df._1), how='leftouter').drop(
    '_1').withColumnRenamed('_2', 'dateyear').sort('dateyear')
new_week_calendar_df = new_calendar_df.withColumn('week_of_year', weekofyear(new_calendar_df.dateyear)).drop('dateyear')

# FIRST JOIN
# sales.dateId should be equal to calendar.dateId to get weekNumber and year
# Use .drop function to drop the column after joining the dataframe because we dont need it this use case
sales_calendar_df = sales_df.join(new_week_calendar_df, (new_week_calendar_df.datekey == sales_df.dateId), how='leftouter').drop(
    'datekey')


# SECOND JOIN
# sales.productId = product.productId
product_sales_calendar_df = sales_calendar_df.join(product_df, (sales_df.productId == product_df.productid),
                                                   how='leftouter').drop('productid')

# THIRD JOIN
# sales.storeId = store.storeId
store_product_sales_calendar_df = product_sales_calendar_df.join(store_df, (sales_df.storeId == store_df.storeid),
                                                                 how='leftouter').drop('storeid')

# cleanse for null
store_product_sales_calendar_df.na.fill(0)
store_sales_with_double_type = store_product_sales_calendar_df.netSales.cast('double')
store_sales_unit_int_type = store_product_sales_calendar_df.salesUnits.cast('int')
new_store_sales_with_double_df = store_product_sales_calendar_df.withColumn('netSalesDouble',
                                                                            store_sales_with_double_type).withColumn('salesUnitsInt',store_sales_unit_int_type)
new_store_sales_with_double_df.orderBy(new_store_sales_with_double_df.division,
                                       new_store_sales_with_double_df.gender,
                                       new_store_sales_with_double_df.category,
                                       new_store_sales_with_double_df.channel,
                                       new_store_sales_with_double_df.datecalendaryear)


new_store_sales_with_double_df.groupBy(new_store_sales_with_double_df.division,
                                       new_store_sales_with_double_df.gender,
                                       new_store_sales_with_double_df.category,
                                       new_store_sales_with_double_df.channel,
                                       new_store_sales_with_double_df.datecalendaryear,
                                       new_store_sales_with_double_df.week_of_year).agg(
    F.sum(new_store_sales_with_double_df.netSalesDouble),F.sum(new_store_sales_with_double_df.salesUnitsInt)).withColumnRenamed('SUM(netSalesDouble)', 'weekly_sales')\
    .withColumnRenamed('SUM(salesUnitsInt)', 'weekly_units').withColumn('key', F.concat(col("division"),col("gender"),col("category"),col('channel')))\
.orderBy('key').show()


# enrich with unique key
# "uniqueKey": "RY18_DIGITAL_DIV1_KIDS_EQIPMENT",
# "division": "DIV1",
# "gender": "KIDS",
# "category": "EQUIPMENT",
# "channel": "DIGITAL",
# "year": "RY18”,



# multiple json files based on unique kyes

spark.stop()