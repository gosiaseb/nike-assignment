import datetime
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import weekofyear, col, concat_ws, to_json, isnull, collect_list, array
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType


class Assignment:
    def __init__(self, calendar_data_file, product_data_file, sales_data_file, store_data_file, output_data_file):
        self.calendar_data_file = calendar_data_file
        self.product_data_file = product_data_file
        self.sales_data_file = sales_data_file
        self.store_data_file = store_data_file
        self.output_data_file = output_data_file

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

        calendar_df = spark.read.option("header", True).csv(self.calendar_data_file)
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
        new_store_sales_with_double_df = store_product_sales_calendar_df \
            .withColumn('netSales', col('netSales').cast('Double')) \
            .withColumn('salesUnits', col('salesUnits').cast('int'))

        # enrich key using concat_ws
        key_store_sales = new_store_sales_with_double_df.withColumn('key', concat_ws('_',
                                                                                     new_store_sales_with_double_df.division,
                                                                                     new_store_sales_with_double_df.gender,
                                                                                     new_store_sales_with_double_df.category,
                                                                                     new_store_sales_with_double_df.channel,
                                                                                     new_store_sales_with_double_df.datecalendaryear))
        key_store_sales.cache()
        # aggregate netSales and salesUnits on unique datecalendaryear and week_of_year
        # creating key
        output_data_for_known_weeks = key_store_sales.groupBy(key_store_sales.division,
                                                              key_store_sales.gender,
                                                              key_store_sales.category,
                                                              key_store_sales.channel,
                                                              key_store_sales.datecalendaryear,
                                                              key_store_sales.week_of_year,
                                                              key_store_sales.key) \
            .agg(F.sum(key_store_sales.netSales),
                 F.sum(key_store_sales.salesUnits)) \
            .withColumnRenamed('SUM(netSales)', 'weekly_sales') \
            .withColumnRenamed('SUM(salesUnits)', 'weekly_units') \
            .orderBy('key')

        distinct_key_df = key_store_sales.select('key').distinct()
        all_weeks_key_df = calendar_with_week_number_df.crossJoin(distinct_key_df).select('key', 'week_of_year') \
            .withColumnRenamed('week_of_year', 'week_number')

        all_data_with_missing_weeks_df = all_weeks_key_df.join(output_data_for_known_weeks, (
                all_weeks_key_df.key == output_data_for_known_weeks.key) & (
                                                                       all_weeks_key_df.week_number == output_data_for_known_weeks.week_of_year),
                                                               how='leftouter').drop(all_weeks_key_df.key)

        all_data_with_missing_weeks_df = all_data_with_missing_weeks_df.fillna(0,
                                                                               subset=['weekly_sales', 'weekly_units'])
        all_data_with_missing_weeks_df.cache()

        pivoted_netSales_on_weekNumbers_df = all_data_with_missing_weeks_df.groupBy('key') \
            .pivot('week_number').sum('weekly_sales').filter(~isnull('key')).fillna(0)
        pivoted_netSales_on_weekNumbers_df.cache()

        pivoted_salesUnits_on_weekNumbers_df = all_data_with_missing_weeks_df.groupBy('key') \
            .pivot('week_number').sum('weekly_units').filter(~isnull('key')).fillna(0)
        pivoted_salesUnits_on_weekNumbers_df.cache()

        json_df_netSales = self.create_json_string_for_each_row(pivoted_netSales_on_weekNumbers_df, "Net Sales")
        json_df_netSales = json_df_netSales.withColumnRenamed("dataRows", "net_sales")
        json_df_salesUnits = self.create_json_string_for_each_row(pivoted_salesUnits_on_weekNumbers_df, "Sales Units")
        json_df_salesUnits = json_df_salesUnits.withColumnRenamed("dataRows", "sales_units")
        final_data_df = key_store_sales.select('key', 'division', 'gender', 'category', 'channel',
                                               'datecalendaryear').distinct() \
            .join(json_df_netSales, (key_store_sales.key == json_df_netSales.uniqueKey), how='leftouter').drop(
            "uniquekey") \
            .join(json_df_salesUnits, (key_store_sales.key == json_df_salesUnits.uniqueKey), how='leftouter').drop(
            "uniquekey")
        final_data_df_array = final_data_df.select('key', 'division', 'gender', 'category', 'channel',
                                                   'datecalendaryear',
                                                   array(final_data_df.net_sales, final_data_df.sales_units).alias(
                                                       "dataRows"))
        final_data_df_array.repartition("key").write.json(self.output_data_file)
        return

    def get_json_string(self, row_id):
        def map_fn(row):
            week_dict = {
                "dataRows": {"dataRow": json.dumps(
                    {"W" + str(week_number): float(value) for week_number, value in enumerate(row) if
                     week_number > 0}), "rowId": row_id}, "uniqueKey": row[0]}
            return week_dict

        return map_fn

    def create_json_string_for_each_row(self, pivoted_df, row_id):
        json_string_for_each_row = pivoted_df.rdd.map(self.get_json_string(row_id)).toDF(schema=StructType([
            StructField("uniqueKey", StringType(), True),
            StructField("dataRows", StructType([
                StructField("rowId", StringType(), True),
                StructField("dataRow", StringType(), True)]), True)]))
        json_string_for_each_row = json_string_for_each_row.withColumn("dataRows", to_json("dataRows"))

        return json_string_for_each_row
