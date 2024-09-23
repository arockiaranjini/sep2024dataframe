import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

import org.apache.spark.sql.catalyst.util.DropMalformedMode
////read from datasources
object defineSchema extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
  //programmatic approach
  val ordersSchema1 = StructType(List(
    StructField("orderid", IntegerType),
    StructField("orderdate", TimestampType),
    StructField("customerid", IntegerType),
    StructField("status", StringType)
  ))
  //DDL string approach
 // val ordersSchemaDDL = "orderid Int, orderdate String, custid Int, ordstatus String"
  val ordersDf = spark.read
    .format("csv")
    .option("header",true)
    .schema(ordersSchema1)
    //.schema(ordersSchemaDDL)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/orders.csv")
    .load
  ordersDf.printSchema()
  ordersDf.show()
}


