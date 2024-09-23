import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions._

object SparkJoinAlias extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  val ordersDf = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/order1.csv")
    .load
  ordersDf.show()
  val customerDf = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/cust1.csv")
    .load
  customerDf.show()
  // val joinCondition = ordersDf.col("cust_id")=== customerDf.col("cust_id")

  //val joinType ="inner"
  //outer,right,left

  //val joinDf = ordersDf.join(customerDf,joinCondition,joinType)


  val joinDf = ordersDf.as("o").join(customerDf.as("c"), col("o.cust_id") === col("c.cust_id"),"inner")

   // .join(Prod.as("p"), $"o.cust_id" === $"p.cust_id")
    .select(col("c.cust_id"), col("o.cust_id"), col("c.cust_id"))

  //val joinDf = ordersDf.join(customerDf,ordersDf.col("cust_id")===customerDf.col("cust_id"),"inner")

  joinDf.show

  spark.stop()
}

