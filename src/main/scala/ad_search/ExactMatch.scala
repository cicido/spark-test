package ad_search

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
/*
author: duanxiping@meizu.com
date: 2016-10-08
file: ExactMatch.scala
 */

class ExactMatch {

}

object ExactMatch{
  val bid_table: String = "ad_recommend.bdl_t_poster_cat_mapping"
  val search_table: String = "app_center.mdl_fdt_apps_search_click_stat_d"
  val exact_match_table: String = "default.dxp_exact_match_word_count"
  val exact_include_table: String = "default.dxp_exact_include_word_count"
  val syno_include_table: String = "default.dxp_syno_include_word_count"
  val core_include_table: String = "default.dxp_core_include_word_count"

  var hiveContext: HiveContext = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null

  def initSpark(appname:String): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName(appname)
    sc = new SparkContext(sparkConf)
    hiveContext = new HiveContext(sc)
    sqlContext = new SQLContext(sc)
  }

  def main(args: Array[String]): Unit = {
      val sdate = args(0).toLong
      val edate = args(1).toLong
      //initSpark("Ad-Search")
      //getExactMatchPVR(sdate, edate, bid_table,search_table)
      //getBidWordData("aa",1,1)
      getSearchWordData("bb",1,1)


  }

  /*
  sdate: start date
  edate: end date
  get the time range string
   */
  def getTimeStr(sdate:Long, edate:Long): String = {
    val time_range:String = (sdate,edate) match {
      case (0,0) => ""
      case (0,edate) => s" stat_date < $edate "
      case (sdate,0) => s" stat_date >=$sdate "
      case (sdate,edate) => s" stat_date >= $sdate and stat_date < $edate "
    }
    println(time_range)
    time_range
  }

  def getBidWordData(tbname:String,sdate:Long, edate:Long): DataFrame = {
    val time_range: String = getTimeStr(sdate, edate)
    var bid_sql:String = s"select distinct(trim(fkey_words)) as bid_word from $bid_table " +
        "where trim(fkey_words) is not null"
    if(time_range != ""){
      bid_sql = s"select distinct(trim(fkey_words)) as bid_word from $tbname " +
        s"where ${time_range} and trim(fkey_words) is not null"
    }
    println(bid_sql)
    hiveContext.sql(bid_sql)
  }

  def getSearchWordData(tbname:String,sdate:Long, edate:Long): DataFrame = {
    val time_range: String = getTimeStr(sdate, edate)
    var search_sql:String = s"select trim(keyword) as search_word,count(1) as cnt from $search_table where " +
      s"trim(keyword) is not null group by trim(keyword)"
    if(time_range != ""){
      search_sql = s"select trim(keyword) as search_word,count(1) as cnt from $search_table where " +
        s"$time_range and trim(keyword) is not null group by trim(keyword)"
    }
    println(search_sql)
    hiveContext.sql(search_sql)
  }


  def getExactMatchPVR(sdate:Long, edate: Long, bid_table:String, search_table:String) = {
    //import hiveContext.implicits._
    val bid_sql:String = s"select distinct(trim(fkey_words)) as bid_word from $bid_table where trim(fkey_words) is not null"
    val search_sql:String = s"select trim(keyword) as search_word,count(1) as cnt from $search_table where " +
      s"stat_date >= $sdate and stat_date < $edate and trim(keyword) is not null group by trim(keyword)"
    val bid_word_df: DataFrame = hiveContext.sql(bid_sql)
    val search_word_df: DataFrame = hiveContext.sql(search_sql)


    println("bid_word_df.size:" + bid_word_df.count())
    println("search_word_df.size:" + search_word_df.count())
    // get all search word
    //val search_word_num: DataFrame = search_word_df.agg(sum($"cnt"))

    /*
    //exact match
    val tmptable: String = "dxp_tmp_table"
    search_word_df.join(bid_word_df, search_word_df("search_word") === bid_word_df("bid_word"))
        .select("search_word","cnt").registerTempTable(tmptable)

    //create and load
    val create_table_sql:String = s"create table if not exists $exact_match_table " +
      "(keyword string, cnt bigint) partitioned by (stat_date bigint) stored as textfile"
    hiveContext.sql(create_table_sql)

    val insert_sql:String = s"insert overwrite table $exact_match_table partition(stat_date = $sdate) select * from $tmptable"
    hiveContext.sql(insert_sql)
    */
    val bid_word_arr: Array[String] = bid_word_df.rdd.
      map(r=>{
        r.getAs[String]("bid_word")
      }).collect()

    val tmp_exact_include_table: String = "dxp_tmp_exact_include_table"
    val join_exact_include_rdd = search_word_df.rdd.repartition(500).
      filter(r=>{
        val sword = r.getAs[String]("search_word")
        var found = false
        var i = 0

        while(!found && i < bid_word_arr.length){
          //if(sword.contains(bid_word_arr(i))){
          if(bid_word_arr(i).contains(sword)){
            found = true
          }
          i += 1
        }
        found
      })

    hiveContext.createDataFrame(join_exact_include_rdd, search_word_df.schema).registerTempTable(tmp_exact_include_table)

    val create_exact_include_table_sql:String = s"create table if not exists $exact_include_table " +
      "(search_word string,cnt bigint) partitioned by (stat_date bigint) stored as textfile"
    hiveContext.sql(create_exact_include_table_sql)

    val insert_exact_include_sql:String = s"insert overwrite table $exact_include_table partition(stat_date = $sdate) " +
      s"select * from $tmp_exact_include_table"
    hiveContext.sql(insert_exact_include_sql)
    /*
    val join_all_df = search_word_df.join(bid_word_df)
    println("join_all_df.size:" + join_all_df.count())
    join_all_df.printSchema()
    //exact include
    val join_all_filter_df = join_all_df.filter(join_all_df("search_word").contains(join_all_df("bid_word")))
    println("join_all_filter_df.size:" + join_all_filter_df.count())
    join_all_filter_df.registerTempTable(tmp_exact_include_table)
    */


  }
}

