import io.netty.handler.codec.spdy.DefaultSpdyDataFrame
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

object Main3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Find Duplication").getOrCreate()
    import spark.implicits._
    val black = spark.sql(" select a.created_at,a.uuid,a.phone_no from xyqb.user a join blacklist.black_type_list_new b on a.phone_no =b.phone where a.created_at >'2017-08-00 00:00:00' and a.created_at <'2017-09-00 00:00:00' limit 500")
    val white = spark.sql("select c.created,c.auuid,c.aphone from ( select a.created_at as created,a.uuid as auuid,a.phone_no as aphone , b.phone as bphone from (select phone_no,uuid,created_at from xyqb.user where created_at >'2017-08-00 00:00:00' and created_at <'2017-09-00 00:00:00') a left join (select phone from blacklist.black_type_list_new) b on a.phone_no = b.phone )c where c.bphone is null limit 500")
    //  black.createGlobalTempView("db_black")
    //  white.createGlobalTempView("db_white")
    //
    //  spark.sql("select a.receiverphone, a.phone from deeplearning.t_user_relation_phone a join xyqb.user b on a.phone =b.phone_no join (select a.created_at,a.uuid,a.phone_no from " +
    //    "xyqb.user a join blacklist.black_type_list_new b on a.phone_no =b.phone where a.created_at " +
    //    ">'2017-08-00 00:00:00' and a.created_at <'2017-09-00 00:00:00' limit 500) c on a.phone =c.phone_no")
    val blackArray = black.collect()
    val whiteArray = white.collect().union(blackArray)
    var hashMap =  new mutable.HashMap[String,Double]()
    for (x <-whiteArray){
      if(x.size==3){

        val time = x.getAs[String](0)
        val uuid = x.getAs[String](1)
        val phone = x.getAs[String](2).trim
        val findphoneA1: Dataset[Row] = spark.sql(s"select a.receiverphone, a.phone from deeplearning.t_user_relation_phone a join xyqb.user b on a.phone =b.phone_no  where ((a.phone='${phone}' and a.receiverphone !='${phone}'  ) or (a.phone !='${phone}' and a.receiverphone ='${phone}')) and (date_format(from_unixtime(unix_timestamp(cast(a.last_call_time as string),'yyyyMMDDHHmmSS')),'yyyy-MM-DD HH:mm:ss') <'${time}') and a.dt='20171008'")
        val findphoneA2: Dataset[Row] = spark.sql(s"select a.receiverphone, a.phone from deeplearning.t_user_relation_phone a join xyqb.user b on a.receiverphone =b.phone_no  where ((a.phone='${phone}' and a.receiverphone !='${phone}' ) or (a.phone !='${phone}' and a.receiverphone ='${phone}')) and (date_format(from_unixtime(unix_timestamp(cast(a.last_call_time as string),'yyyyMMDDHHmmSS')),'yyyy-MM-DD HH:mm:ss') <'${time}') and a.dt='20171008'")
        val findphoneA = findphoneA1.union(findphoneA2).collect()

        val hashA = new mutable.HashSet[String]()
        if (findphoneA.size != 0) {
          for (a <- findphoneA) {
            if (a.size >= 2) {
              val r = a.getAs[String](0)
              val p = a.getAs[String](1)
              hashA.add(r)
              hashA.add(p)
            }
          }
        }
        var max = 0.0
        if (hashA.contains(phone)) hashA.remove(phone)
        for(a <- hashA){
          val hashB = new mutable.HashSet[String]()
          val findphoneB1: Dataset[Row] = spark.sql(s"select a.receiverphone, a.phone from deeplearning.t_user_relation_phone a join xyqb.user b on a.phone =b.phone_no  where ((a.phone='${a}' and a.receiverphone !='${a}' ) or (a.phone !='${a}' and a.receiverphone ='${a}')) and (date_format(from_unixtime(unix_timestamp(cast(a.last_call_time as string),'yyyyMMDDHHmmSS')),'yyyy-MM-DD HH:mm:ss') <'${time}') and a.dt='20171008'")
          val findphoneB2: Dataset[Row] = spark.sql(s"select a.receiverphone, a.phone from deeplearning.t_user_relation_phone a join xyqb.user b on a.receiverphone =b.phone_no  where ((a.phone='${a}' and a.receiverphone !='${a}' ) or (a.phone !='${a}' and a.receiverphone ='${a}')) and (date_format(from_unixtime(unix_timestamp(cast(a.last_call_time as string),'yyyyMMDDHHmmSS')),'yyyy-MM-DD HH:mm:ss') <'${time}') and a.dt='20171008'")
          val findphoneB = findphoneB1.union(findphoneB2).collect()
          if (findphoneB.size != 0) {
            for (a <- findphoneB) {
              if (a.size >= 2) {
                val r = a.getAs[String](0)
                val p = a.getAs[String](1)
                hashB.add(r)
                hashB.add(p)
              }
            }
          }
          var count = 0
          for (a <- hashB) {
            if (hashA.contains(a)) {
              count = count + 1
            }
          }
          max = math.max(max, count / hashA.size)
        }
        hashMap.put(phone,max)
        //println(findphoneA.count())
      }
    }
    hashMap.foreach(println(_))
    spark.sparkContext.parallelize(hashMap.toList,1).saveAsTextFile("/app/user/data/deeplearning/results/MostCommon")
  }
}
