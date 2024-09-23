import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object defineSchemaJoin extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()
  val ordersSchemaDDL = "order_id Int,customer_id Int,order_status Int,order_date Date,required_date Date,shipped_date Date,store_id Int,staff_id Int"
  val ordersDf = spark.read
    .format("csv")
    .option("header",true)
    .schema(ordersSchemaDDL)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/csvfolder/orders.csv")
    .load
  ordersDf.show()
  val customersSchemaDDL="customer_id Int,first_name String,last_name String,phone Int," +
    "email String,street String,city String,state String,zip_code Int"

  val customerDf = spark.read
    .format("csv")
    .option("header",true)
    .schema(customersSchemaDDL)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/csvfolder/customers.csv")
    .load
  customerDf.show()

  val orderitemsSchemaDDL="orders_id Int,item_id Int,product_id Int,quantity Int,list_price Float,discount Float"
  val orderitemsDf = spark.read
    .format("csv")
    .option("header", true)
    .schema(orderitemsSchemaDDL)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/csvfolder/order_items.csv")
    .load
  orderitemsDf.show()
  // val renamedorderitemsDf=orderitemsDf.withColumnRenamed("order_id","orders_id")
  //.withColumnRenamed("list_price","lists_price")
  //.show

  val joinDf = ordersDf
    .join(customerDf,ordersDf.col("customer_id")=== customerDf.col("customer_id"),"inner")
    .join(orderitemsDf,ordersDf.col("order_id")===orderitemsDf.col("orders_id"),"inner")

  //joinDf.show
  joinDf.select("order_id","order_date","first_name","email","product_id","list_price").show

  spark.stop()
}

