import io.netty.handler.codec.spdy.DefaultSpdyDataFrame
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

object Main{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Find Duplication").getOrCreate()
    //val black = spark.sql(" select a.created_at,a.uuid,a.phone_no from xyqb.user a join blacklist.black_type_list_new b on a.phone_no =b.phone where a.created_at <'2017-09-00 00:00:00' and a.created_at >'2017-08-00 00:00:00' limit 500 ").rdd.cache()
    //val white = spark.sql("select c.created,c.auuid,c.aphone from ( select a.created_at as created,a.uuid as auuid,a.phone_no as aphone , b.phone as bphone from (select phone_no,uuid,created_at from xyqb.user where created_at <'2017-09-00 00:00:00' and created_at >'2017-08-00 00:00:00') a left join (select phone from blacklist.black_type_list_new) b on a.phone_no = b.phone )c where c.bphone is null limit 500").rdd.cache()
    val file =spark.sparkContext.textFile("/app/user/data/deeplearning/results/test.txt").map(x =>{
      val parts = x.split("\t")
      (parts(4),(parts(0),parts(1)))
    })
    //  black.createGlobalTempView("db_black")
    //  white.createGlobalTempView("db_white")
    //  spark.sql("select a.receiverphone, a.phone from deeplearning.t_user_relation_phone a join xyqb.user b on a.phone =b.phone_no join (select a.created_at,a.uuid,a.phone_no from " +
    //  "xyqb.user a join blacklist.black_type_list_new b on a.phone_no =b.phone where a.created_at " +
    //  ">'2017-08-00 00:00:00' and a.created_at <'2017-09-00 00:00:00' limit 500) c on a.phone =c.phone_no")
    val alluser = spark.sql("select a.created_at,a.uuid,a.phone_no from xyqb.user a  where a.created_at <'2017-08-00 00:00:00'").rdd.map(x =>
      if(x.size==3){
        (x.getAs[String](2)," ")
      }else{
        ("","")
      }
    ).distinct()

    val phoneRelation1  = spark.sql("select a.phone,a.receiverphone , date_format(from_unixtime(unix_timestamp(cast(a.first_call_time as string),'yyyyMMDDHHmmSS')),'yyyy-MM-DD HH:mm:ss') as time from deeplearning.t_user_relation_phone a  where a.dt='20171008' and a.first_call_time is not null").rdd
    val phoneRelation2  = spark.sql("select a.receiverphone,a.phone , date_format(from_unixtime(unix_timestamp(cast(a.first_call_time as string),'yyyyMMDDHHmmSS')),'yyyy-MM-DD HH:mm:ss') as time from deeplearning.t_user_relation_phone a  where a.dt='20171008' and a.first_call_time is not null").rdd
    val phoneRelation =phoneRelation1.union(phoneRelation2).map(x=>(x.getAs[String](0),(x.getAs[String](1),x.getAs[String](2)))).filter(x =>x._1!=x._2._1).cache()
//
//    val user = spark.sparkContext.parallelize(black.take(500).union(white.take(500))).map(x => {
//        //time =if (x.getAs[String](0) =="") "0" else
//        val time = if (x.getAs[String](0) ==null) "0" else x.getAs[String](0)
//        val uuid = x.getAs[String](1)
//        val phone = x.getAs[String](2).trim
//
//        (phone,(uuid,time))

    val user=file.join(phoneRelation).filter(x => {
      //x._2._1._2 > x._2._2._2
      try{
        x._2._1._2 > x._2._2._2
      }catch{
        case e =>false
      }
    }).groupByKey().repartition(1000).cache()

    val userPart = user.map(x =>{
      var list = new mutable.HashSet[String]()
      //   uuid,time    receiverphone,time
      //val value: Iterable[((String, String), (String, String))] = x._2
      x._2.foreach(y =>list.add(x._1+","+y._1._2+","+y._2._1))
      (x._1,list)
    })

    val userPart1 = user.map(x =>{
      var list = new mutable.HashSet[String]()
      //   uuid,time    receiverphone,time
      //  val value: Iterable[((String, String), (String, String))] = x._2
      x._2.foreach(y =>list.add(y._2._1))
      (x._1,list)
    })

    val userP1 = userPart.flatMap(x =>(x._2)).map(x =>{
      val parts =x.split(",")
      val time  =if (parts(1)==null) "0" else parts(1)
      //B用户，A用户,时间
      (parts(2),(parts(0),time))
    })

    println(userP1.count())
    val userP =userP1.join(alluser).join(phoneRelation).filter(x=>
      try
      {x._2._1._1._2 > x._2._2._2}
        catch {
        case e =>false
      }
      ).groupByKey()

    val userPa= userP.map(x =>{
        var list = new mutable.HashSet[String]()
      // A,time ' ' , user time
//      val value: Iterable[(((String, String), String), (String, String))] = x._2
//      val t =value
        x._2.foreach(y =>list.add(y._2._1))
       list.add(x._1)
        (x._1,list)
      }).collect().toMap

    println(userPa.size)

    val pa: Map[String, mutable.HashSet[String]] = userPa
    println("first size")
    println(pa.head._2.size)
    pa.head._2.foreach(println(_))
    phoneRelation.unpersist()
    userP1.map(x =>x._1).foreach(println(_))

    val userpa = spark.sparkContext.broadcast(userPa)

    val results =userPart1.map(x =>{
      var max =0.0
      for( y <- x._2){
        if(userpa.value.contains(y)){
          var count =0.0
          val all = userpa.value.getOrElse(y,mutable.HashSet[String]())
          for (z <- all){
            if(x._2.contains(z)){
              count =count+1
            }
          }
          max =Math.max(max,count/x._2.size.toDouble)
        }
      }
      (x._1,max)
    }).repartition(1)
    results.collect().foreach(println(_))
    results.saveAsTextFile("/app/user/data/deeplearning/results/MostCommon1")

//    for(x <- userPart){
//      val size = x._2.size
//      spark.sparkContext.accumulator[int]()
//      for(y <- x._2){
//        glom.foreach(x =>{
//          for(y <- x){
//            y._1
//          }
//
//        })
//      }
    }


}
