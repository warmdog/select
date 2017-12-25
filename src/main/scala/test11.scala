import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object test11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val tes = Map[String,Int]()
    //import sparkSession.implicits._
    val sc = new SparkContext("local","wordcount",conf)
    val file =sc.textFile("d:/Users/Desktop/工作日志/test.txt").map(x =>{
      val parts = x.split("\t")
      (parts(4),(parts(0),parts(1)))
    })
    file.take(1).foreach(println(_))
  }
}
