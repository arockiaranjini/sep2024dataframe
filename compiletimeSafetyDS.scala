import org.apache.spark.sql.SparkSession
import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import java.sql.Timestamp
case class OrderData(order_id:Int,order_date:String,order_customer_id:Int,order_status:String)
object compiletimeSafetyDS extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf =new SparkConf()
  sparkConf.set("spark.appname","myfirstapplication")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDf:Dataset[Row]=spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv("C:/Users/MCT/Desktop/sparkdataset/orders.csv")
  import spark.implicits._
  val ordersDs =ordersDf.as[OrderData]
  ordersDs.filter(x=>x.order_id<10)
  //ordersDf.filter(x=>x.order_ids<10)     ///try for checking
  //Dataset[Row]=>Dataframes
  //Dataset[object]=>Datasets

  //it shows error during runtime it is not compiletime safety
  scala.io.StdIn.readLine()
  spark.stop()
}

