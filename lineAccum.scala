import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.util.LongAccumulator
object lineAccum extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc =new SparkContext("local[*]","wordCount")
  val myRdd = sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/accum.txt")
  val myAccumulator1= sc.longAccumulator("blank lines accumulator")
  val myAccumulator2 = sc.longAccumulator("count words in line")
  myRdd.foreach( x => if(x== "" ) myAccumulator1.add(1))
  println("empty lines ="+ myAccumulator1.value)
  myRdd.foreach(x => if (x.contains("big")) myAccumulator2.add(1)) //count word in lines

  println("Number of lines containing 'big': " + myAccumulator2.value)
}
