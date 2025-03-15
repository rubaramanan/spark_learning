from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType 
from pyspark.sql import functions as func

session = SparkSession.builder.appName('cx total').getOrCreate()

schema = StructType([
    StructField('client_id', IntegerType(), True),
    StructField('item_id', FloatType(), True),
    StructField('sales', FloatType(), True)
])

df = session.read.schema(schema).csv('/opt/bitnami/spark/data/customer-orders.csv')

df = df.select('client_id', 'sales')

tot_sale = df.groupby('client_id').agg(func.round(func.sum('sales'),2).alias('total_sales')).sort('total_sales')

tot_sale.show(tot_sale.count())

session.stop()