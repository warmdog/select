import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object Main1 {
//  val filePath ="/app/user/data/deeplearning/t_user_relation_phone/dt=20171008/000*"
//  val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Spark GraphXXX").getOrCreate()
    val fileRDD: RDD[Row] = spark.sql("select phone,receiverphone,total_num,durations_time from deeplearning.t_user_relation_phone where dt='20170606'").rdd
    val edgeRDD=fileRDD.map(x =>{
      val phone = x.getAs[String](0).toLong
      val receiverphone = x.getAs[String](1).toLong
      val number = x.getAs[Long](2)
      val durations_time = x.getAs[Long](3)

      (phone,receiverphone,number,durations_time)
    }).filter(x =>x._4>10).map(x =>{
      Edge(x._1,x._2,x._3)
    }).filter(x =>x.dstId!=x.srcId)
    // 黑名单最终日期  2017-10-19 13:58:45
    // 通话记录最终日期 2017-10-08
    val blackDataSet: Dataset[Row] = spark.sql("select phone from blacklist.black_type_list_new where createdate<'2017-06-07 00:00:00'")
    val xyqbDataSet:Dataset[Row] = spark.sql("select phone_no from xyqb.user where created_at<'2017-06-07 00:00:00'")
    val blackRDD: RDD[Row] = blackDataSet.rdd
    //println(s"blackRDD: ${blackRDD.count()}")
    val xyqbRDD = xyqbDataSet.rdd
    val regex = "[0-9]".r
    val vertexRDD= blackRDD.map(x => {
      val s = x.toString().trim
      //hive 获取多行数据方法 val value = x.getAs[String](0)
      //val num = x.getAs[Long](1)
      ((regex findAllIn s).mkString("").toLong, 1)
    })
    val vertexRDD1: RDD[(VertexId, Int)] = xyqbRDD.map(x => {
      val s = x.toString()
      //hive 获取多行数据方法 val value = x.getAs[String](1)
      //val num = x.getAs[Long](2)
      ((regex findAllIn s).mkString("").toLong, 1)
    }).leftOuterJoin(vertexRDD).map(x =>{
      (x._1,x._2._2.getOrElse(2))
    })
    //println(s"blackRDD: ${vertexRDD1.count()}")
    //println(s"xqybRDDcount: ${vertexRDD1.top(10).foreach(println(_))}" )
    // 1 黑名单 2 xyqb用户
    val value: Graph[Int, Long] = Graph.fromEdges(edgeRDD, 0)
    //Graph(vertexRDD,edgeRDD)
    val graph = value.outerJoinVertices(vertexRDD1) { (id, oldAttr, outDegOpt) =>
      //      outDegOpt match {
      //        case Some(outDegOpt) => outDegOpt
      //        case None =>oldAttr
      //      }
      outDegOpt.getOrElse(0)
    }
    val black  = graph.subgraph(vpred = (vertexId, value) => (value== 1 || value==2))
    val result =black
    println("new Graph:")
    result.vertices.top(10).foreach(println(_))

  }
  def triangle(graph: Graph[Int,Long]):Unit={
    NewTriangleCount.run(graph).vertices.map(x =>{
      val sb = new StringBuilder()

      try{
        val res = x._2.toList
        var i =0
        for (n<-res){
          //x._2.map(_.mkString("\t")).mkString("\n")
          if(i==res.length-1){

            sb.append(x._1 + "\t" + n.mkString("\t"))
          }else {
            sb.append(x._1 + "\t" + n.mkString("\t")+"\n")
          }
          i= i+1
        }
      }catch {
        case e:NullPointerException =>x._1 +"\t" +null
      }
      sb.toString()
    }).filter(x =>x.length!=0).repartition(1).saveAsTextFile("/app/user/data/deeplearning/results/triangleVertices")


  }
}
