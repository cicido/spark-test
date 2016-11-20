package ad_search

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.{Term => HanLPTerm}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, Set => MuSet}

/*
author: duanxiping@meizu.com
date: 2016-10-08
file: ExactMatch.scala
 */

object ExactMatch {
  val bid_table: String = "ad_recommend.bdl_t_poster_cat_mapping"
  val search_table: String = "app_center.mdl_fdt_apps_search_click_stat_d"
  val exact_match_table: String = "default.dxp_exact_match_word_count"
  val exact_include_table: String = "default.dxp_exact_include_word_count"
  val syno_include_table: String = "default.dxp_syno_include_word_count"
  val core_include_table: String = "default.dxp_core_include_word_count"
  val query_bid_word_table: String = "default.dxp_query_bid_word_data"

  var hiveContext: HiveContext = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null

  def initSpark(appname: String): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName(appname)
    sc = new SparkContext(sparkConf)
    hiveContext = new HiveContext(sc)
    sqlContext = new SQLContext(sc)
  }

  def main(args: Array[String]): Unit = {
    val mtype = args(0)
    val sdate = args(1).toLong
    val edate = args(2).toLong
    val pdate = args(3).toLong
    initSpark("Ad-Search")

    //get hive table data
    val bid_word_df: DataFrame = getBidWordData(bid_table, 0, edate)
    val search_word_df: DataFrame = getSearchWordData(search_table, sdate, edate)
    println("bid_word_df.size:" + bid_word_df.count())
    println("search_word_df.size:" + search_word_df.count())

    //computing pvr according to the mtype
    mtype match {
      case "exact-match" => getExactMatchPVR(bid_word_df, search_word_df, exact_match_table, pdate)
      case "exact-include" => getExactIncludePVR(bid_word_df, search_word_df, exact_include_table, pdate)
      case "syno-include" => getSynoIncludePVR(bid_word_df, search_word_df, syno_include_table, pdate)
      case "core-include" => getCoreIncludePVR(bid_word_df, search_word_df, core_include_table, pdate)
      case "get-data" => getWordPair(bid_word_df,search_word_df,query_bid_word_table, pdate)
    }
  }

  /*
  getExactMatchPVR
   */

  def segMsg(msg: String): MuSet[String] = {
    val wordSet = MuSet.empty[String]
    for (term: HanLPTerm <- HanLP.segment(msg)) {
      wordSet += term.word
    }
    wordSet
  }

  def getWordPair(bid_word_df: DataFrame, search_word_df: DataFrame, output_table:String, pdate:Long){
    val bid_word_arr: Array[(String, MuSet[String])] = bid_word_df.rdd.
      map(r => {
        val wordSet = MuSet.empty[String]
        val sen: String = r.getAs[String]("bid_word")
        for (term: HanLPTerm <- HanLP.segment(sen)) {
          wordSet += term.word
        }
        (sen, wordSet)
      }).collect()

    val join_include_rdd = search_word_df.rdd.repartition(500).map(r=>{
      val bidarr: ArrayBuffer[(String,Int)] =  new ArrayBuffer[(String, Int)]()
      val sen:String = r.getAs[String]("search_word")
      val querySet:MuSet[String] = segMsg(sen)

      for(i <- 0 until bid_word_arr.length) {
        val bidSet:MuSet[String] = bid_word_arr(i)._2
        val bidStr:String = bid_word_arr(i)._1
        val diffSet: MuSet[String] = querySet & bidSet
        if (sen == bidStr) {
          bidarr.append((bidStr, 1))
        } else {
          if (sen.contains(bidStr))
            bidarr.append((bidStr, 2))
          if (diffSet.size == bidSet.size){
            bidarr.append((bidStr, 3))
          }else if(diffSet.size > 0 && diffSet.size == bidSet.size -1){
            bidarr.append((bidStr,4))
          }
        }
      }

      //self define format
      val res:String = if(bidarr.length == 0) "" else bidarr.map(r=>{
        r._1 + "," + r._2
      }).mkString(" ")

      /*
      //json format
      import org.json4s.JsonDSL._
      import org.json4s.jackson.JsonMethods._
      val jsonObj = ("bidlist" -> bidarr.toList.map(r=>{
        (("word"->r._1) ~("tag" ->r._2))
      }))
      val jsonStr:String = if(bidarr.length == 0) "" else compact(render(jsonObj))
      */

      Row(sen,res)
    })

    //print data
    val st = StructType(
      StructField("queryword", StringType, false) ::
        StructField("bidlist", StringType, true) :: Nil)

    val tmptable: String = "dxp_tmp_table"
    hiveContext.createDataFrame(join_include_rdd, st).registerTempTable(tmptable)

    val create_table_sql: String = s"create table if not exists $output_table " +
      "(search_word string,bidlist string) partitioned by (stat_date bigint) stored as textfile"
    hiveContext.sql(create_table_sql)

    val insert_sql: String = s"insert overwrite table $output_table partition(stat_date = $pdate) " +
      s"select * from $tmptable"
    hiveContext.sql(insert_sql)

  }
  def getExactMatchPVR(bid_word_df: DataFrame, search_word_df: DataFrame, output_table: String, pdate: Long) = {
    //import hiveContext.implicits._
    // get all search word
    //val search_word_num: DataFrame = search_word_df.agg(sum($"cnt"))

    //exact match
    val tmptable: String = "dxp_tmp_table"
    search_word_df.join(bid_word_df, search_word_df("search_word") === bid_word_df("bid_word"))
      .select("search_word", "cnt").registerTempTable(tmptable)

    //create and load
    val create_table_sql: String = s"create table if not exists $output_table " +
      "(keyword string, cnt bigint) partitioned by (stat_date bigint) stored as textfile"
    hiveContext.sql(create_table_sql)

    val insert_sql: String = s"insert overwrite table $output_table partition(stat_date = $pdate) select * from $tmptable"
    hiveContext.sql(insert_sql)
  }

  /*
  getExactIncludePVR
   */
  def getExactIncludePVR(bid_word_df: DataFrame, search_word_df: DataFrame, output_table: String, pdate: Long) = {
    //import hiveContext.implicits._
    val bid_word_arr: Array[String] = bid_word_df.rdd.
      map(r => {
        r.getAs[String]("bid_word")
      }).collect()

    val join_include_rdd = search_word_df.rdd.repartition(500).
      filter(r => {
        val sword = r.getAs[String]("search_word")
        var found = false
        var i = 0

        while (!found && i < bid_word_arr.length) {
          if(sword.contains(bid_word_arr(i))){
          //if (bid_word_arr(i).contains(sword)) {
            found = true
          }
          i += 1
        }
        found
      })

    val tmptable: String = "dxp_tmp_table"
    hiveContext.createDataFrame(join_include_rdd, search_word_df.schema).registerTempTable(tmptable)

    val create_table_sql: String = s"create table if not exists $output_table " +
      "(search_word string,cnt bigint) partitioned by (stat_date bigint) stored as textfile"
    hiveContext.sql(create_table_sql)

    val insert_sql: String = s"insert overwrite table $output_table partition(stat_date = $pdate) " +
      s"select * from $tmptable"
    hiveContext.sql(insert_sql)
  }

  /*
  getSynoIncludePVR
   */
  def getSynoIncludePVR(bid_word_df: DataFrame, search_word_df: DataFrame, output_table: String, pdate: Long) = {
    val bid_word_arr: Array[(String, MuSet[String])] = bid_word_df.rdd.
      map(r => {
        val wordSet = MuSet.empty[String]
        val sen: String = r.getAs[String]("bid_word")
        for (term: HanLPTerm <- HanLP.segment(sen)) {
          wordSet += term.word
        }
        (sen, wordSet)
      }).collect()

    val join_include_rdd = search_word_df.rdd.filter(r => {
      val sen: String = r.getAs[String]("search_word")
      val wordSet = MuSet.empty[String]
      for (term: HanLPTerm <- HanLP.segment(sen)) {
        wordSet += term.word
      }
      var found = false
      var i = 0
      while (!found && i < bid_word_arr.length) {
        //if(sword.contains(bid_word_arr(i))){
        if (bid_word_arr(i)._2.subsetOf(wordSet)) {
          found = true
        }
        i += 1
      }
      found
    })

    val tmptable: String = "dxp_tmp_table"
    hiveContext.createDataFrame(join_include_rdd, search_word_df.schema).registerTempTable(tmptable)

    val create_table_sql: String = s"create table if not exists $output_table " +
      "(search_word string,cnt bigint) partitioned by (stat_date bigint) stored as textfile"
    hiveContext.sql(create_table_sql)

    val insert_sql: String = s"insert overwrite table $output_table partition(stat_date = $pdate) " +
      s"select * from $tmptable"
    hiveContext.sql(insert_sql)
  }

  /*
  getCoreIncludePVR
   */
  def getCoreIncludePVR(bid_word_df: DataFrame, search_word_df: DataFrame, output_table: String, pdate: Long) = {
    val bid_word_arr: Array[(String, MuSet[String])] = bid_word_df.rdd.
      map(r => {
        val wordSet = MuSet.empty[String]
        val sen: String = r.getAs[String]("bid_word")
        for (term: HanLPTerm <- HanLP.segment(sen)) {
          wordSet += term.word
        }
        (sen, wordSet)
      }).collect()

    val join_include_rdd = search_word_df.rdd.filter(r => {
      val sen: String = r.getAs[String]("search_word")
      val wordSet = MuSet.empty[String]
      for (term: HanLPTerm <- HanLP.segment(sen)) {
        wordSet += term.word
      }
      var found = false
      var i = 0
      while (!found && i < bid_word_arr.length) {
        val ms: MuSet[String] = bid_word_arr(i)._2
        if((ms.size == 1 && ms.subsetOf(wordSet)) ||
          (ms.size > 1 && ms.&(wordSet).size >= ms.size-1)) {
          found = true
        }
        i += 1
      }
      found
    })

    val tmptable: String = "dxp_tmp_table"
    hiveContext.createDataFrame(join_include_rdd, search_word_df.schema).registerTempTable(tmptable)

    val create_table_sql: String = s"create table if not exists $output_table " +
      "(search_word string,cnt bigint) partitioned by (stat_date bigint) stored as textfile"
    hiveContext.sql(create_table_sql)

    val insert_sql: String = s"insert overwrite table $output_table partition(stat_date = $pdate) " +
      s"select * from $tmptable"
    hiveContext.sql(insert_sql)
  }

  /*
  sdate: start date
  edate: end date
  get the time range string
   */
  def getTimeStr(sdate: Long, edate: Long): String = {
    val time_range: String = (sdate, edate) match {
      case (0, 0) => ""
      case (0, edt) => s" stat_date < $edt "
      case (sdt, 0) => s" stat_date >=$sdt "
      case (sdt, edt) => s" stat_date >= $sdt and stat_date < $edt "
    }
    println(time_range)
    time_range
  }

  def getBidWordData(tbname: String, sdate: Long, edate: Long): DataFrame = {
    val time_range: String = getTimeStr(sdate, edate)
    var bid_sql: String = s"select distinct(trim(fkey_words)) as bid_word from $bid_table " +
      "where trim(fkey_words) is not null"
    if (time_range != "") {
      bid_sql = s"select distinct(trim(fkey_words)) as bid_word from $tbname " +
        s"where $time_range and trim(fkey_words) is not null"
    }
    println(bid_sql)
    hiveContext.sql(bid_sql)
  }

  def getSearchWordData(tbname: String, sdate: Long, edate: Long): DataFrame = {
    val time_range: String = getTimeStr(sdate, edate)
    var search_sql: String = s"select trim(keyword) as search_word,count(1) as cnt from $search_table where " +
      s"trim(keyword) is not null group by trim(keyword)"
    if (time_range != "") {
      search_sql = s"select trim(keyword) as search_word,count(1) as cnt from $search_table where " +
        s"$time_range and trim(keyword) is not null group by trim(keyword)"
    }
    println(search_sql)
    hiveContext.sql(search_sql)
  }

}

