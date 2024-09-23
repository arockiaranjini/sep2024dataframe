import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.Level
import org.apache.log4j.Logger
object writeModes extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    //.enableHiveSupport()
    .getOrCreate()

  val ordersDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/orders.csv")
    .load

  ordersDf.write
    .format("csv")
   // .mode(SaveMode.Overwrite)
   //.mode(SaveMode.Append)
    //.mode(SaveMode.ErrorIfExists)
   .mode(SaveMode.Ignore)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/savefolder03SEP")
    .save()


  spark.stop()
}


