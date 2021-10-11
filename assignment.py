import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import weekofyear, col
from pyspark.sql import functions as F


class Assignment:
    def __init__(self, calendar_data_file, product_data_file, sales_data_file, store_data_file):
        self.calendar_data_file = calendar_data_file
        self.product_data_file = product_data_file
        self.sales_data_file = sales_data_file
        self.store_data_file = store_data_file

    def get_actual_calendar_weeks(self, calendar_df_old):
        pan = calendar_df_old.toPandas()
        list_ofindices = []
        for index, value in enumerate(pan['datecalendarday']):
            if (pan['datecalendarday'][index] == '1'):
                index_count = index
                list_ofindices.append(index_count)
        list_ofindices.append(len(pan['datecalendarday']))
        actual_calendar_week = []
        monthsofyear = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        for i in range(len(list_ofindices) - 1):
            for x in range(list_ofindices[i], list_ofindices[i + 1]):
                dets = datetime.date(int(pan['datecalendaryear'][x]), monthsofyear[i + 1],
                                     int(pan['datecalendarday'][x]))
                actual_calendar_week.append([pan['datekey'][x], dets])

        return actual_calendar_week

    def process_data(self):
        spark = SparkSession.builder.master("local[*]").appName("NikeAssignment").getOrCreate()

        calendar_df = spark.read.option("header", True).csv(self.calendar_data_file).hint("broadcast")
        product_df = spark.read.option("header", True).csv(self.product_data_file).repartition('productId')
        sales_df = spark.read.option("header", True).csv(self.sales_data_file).repartition('productId')
        store_df = spark.read.option("header", True).csv(self.store_data_file).repartition('storeId')

        actual_calendar_week = self.get_actual_calendar_weeks(calendar_df)

        only_calendar_date_df = spark.createDataFrame(actual_calendar_week)
        new_calendar_with_date_df = calendar_df.join(only_calendar_date_df,
                                                     (calendar_df.datekey == only_calendar_date_df._1),
                                                     how='leftouter').drop('_1').withColumnRenamed('_2',
                                                                                                   'dateyear').sort(
            'dateyear')
        calendar_with_week_number_df = new_calendar_with_date_df.withColumn('week_of_year', weekofyear(
            new_calendar_with_date_df.dateyear)).drop(
            'dateyear')

        # FIRST JOIN
        # sales.dateId should be equal to calendar.dateId to get weekNumber and year
        # Use .drop function to drop the column after joining the dataframe because we dont need it this use case
        sales_for_all_weeks_df = sales_df.join(calendar_with_week_number_df,
                                               (calendar_with_week_number_df.datekey == sales_df.dateId),
                                               how='leftouter').drop('datekey')

        # SECOND JOIN
        # sales.productId = product.productId
        product_sales_calendar_df = sales_for_all_weeks_df.join(product_df,
                                                                (sales_df.productId == product_df.productid),
                                                                how='leftouter').drop('productid')

        # THIRD JOIN
        # sales.storeId = store.storeId
        store_product_sales_calendar_df = product_sales_calendar_df.join(store_df,
                                                                         (sales_df.storeId == store_df.storeid),
                                                                         how='leftouter').drop('storeid')

        # cleanse for null
        store_product_sales_calendar_df.fillna(0)

        # change type of 'netSales', 'salesUnits'
        store_sales_with_double_type = store_product_sales_calendar_df.netSales.cast('double')
        store_sales_unit_int_type = store_product_sales_calendar_df.salesUnits.cast('int')

        new_store_sales_with_double_df = store_product_sales_calendar_df \
            .withColumn('netSalesDouble', store_sales_with_double_type) \
            .withColumn('salesUnitsInt', store_sales_unit_int_type)

        # aggregate netSales and salesUnits on unique datecalendaryear and week_of_year
        # creating key
        output_data_for_known_weeks = new_store_sales_with_double_df.groupBy(new_store_sales_with_double_df.division,
                                                                             new_store_sales_with_double_df.gender,
                                                                             new_store_sales_with_double_df.category,
                                                                             new_store_sales_with_double_df.channel,
                                                                             new_store_sales_with_double_df.datecalendaryear,
                                                                             new_store_sales_with_double_df.week_of_year) \
            .agg(F.sum(new_store_sales_with_double_df.netSalesDouble),
                 F.sum(new_store_sales_with_double_df.salesUnitsInt)) \
            .withColumnRenamed('SUM(netSalesDouble)', 'weekly_sales') \
            .withColumnRenamed('SUM(salesUnitsInt)', 'weekly_units') \
            .withColumn('key', F.concat(col("division"), col("gender"), col("category"), col('channel'))) \
            .orderBy('key')

        # TO DO:
        # crossjoin on weekdata to get all weeks
        # pivot data to get all weeks for key
        # multiple json files based on unique kyes

        spark.stop()

        return calendar_df, product_df, sales_df, store_df
