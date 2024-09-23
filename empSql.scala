import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object empSql extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDf=spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","C:/Users/MCT/Desktop/sparkdataset/emp.csv")
    .load
  //ordersDf.show()
  ordersDf.createOrReplaceTempView("emp1")
//ordersDf.createOrReplaceTempView("emp")
  //val resultDf =spark.sql("select * from emp")
  // val resultDf =spark.sql("select count(*) from emp")
  //val resultDf=spark.sql("select emp_id,emp_name from emp where salary>4000")
  //val resultDf=spark.sql("select dept_name,AVG(salary)as salary from emp group by dept_name")
  val resultDf=spark.sql("select dept_name,avg(salary)as salary " +
    "from emp1 " +
    "group by dept_name")
  resultDf.show
  scala.io.StdIn.readLine()
  spark.stop()
}

