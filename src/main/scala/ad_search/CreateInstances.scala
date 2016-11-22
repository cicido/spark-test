package ad_search

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

/**
  * Created by duanxiping on 2016/11/15.
  */
object CreateInstances {
  var hiveContext: HiveContext = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null
  val featureMap = new mutable.HashMap[String,Map[String,Int]]()

  def initSpark(appname: String): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName(appname)
    sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    hiveContext = new HiveContext(sc)
    sqlContext = new SQLContext(sc)
  }

  def main(args: Array[String]): Unit = {
    initSpark("AdSearch-algo")
    val df = makeTestData()
    val new_df = multiColProcess(df,"id",true)
    new_df.write.format("json").save("file:///opt/test.json")
    /*
    val mtype = args(0)
    val sdate = args(1).toLong
    val edate = args(2).toLong
    val pdate = args(3).toLong

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
      case "get-data" => getWordPair(bid_word_df, search_word_df, query_bid_word_table, pdate)
    }
    */
  }

  def makeTestData(): DataFrame = {
    sqlContext.createDataFrame(Seq(
      (0, "young", "male"),
      (1, "young", "female"),
      (2, "old", "male"),
      (3, "old", "female")
    )).toDF("id", "age", "sex")
  }

  //转为cat_id,不做one-hot,以便用于其他非LR算法
  def oneColProcess(col: String) = (df: DataFrame) => {
    val catMap = df.select(col).distinct.map(_.getAs[String](col)).collect.zipWithIndex.toMap
    featureMap(col) = catMap
    val stringToDouble = udf[Double, String] {
      catMap(_)
    }
    df.withColumn(col+"_cat", stringToDouble(df(col)))
  }

  //one-hot编码
  def oneColProcessWithOneHot(col:String) = (df:DataFrame) => {
    val catMap = df.select(col).distinct.map(_.getAs[String](col)).collect.zipWithIndex.toMap
    featureMap(col) = catMap
    val stringToVector = udf[Vector, String] { w =>
      Vectors.sparse(catMap.size, Array(catMap(w)),Array(1))
    }
    df.withColumn(col+"_cat", stringToVector(df(col)))
  }

  //对于非cat类型的字段进行分段处理
  def oneColProcessWithSplit(col:String) = (df:DataFrame) =>{
    //分段范围计算
    val col_range = Array(1,3,5)
    val splitToDouble = udf[Double, Double] { w =>
      var i = 0
      while(i < col_range.length && w < col_range(i)){
        i += 1
      }
      i.toDouble
    }
    df.withColumn(col+"_cat", splitToDouble(df(col)))
  }

  def multiColProcess(df: DataFrame,label:String, needOneHot:Boolean): DataFrame = {
    val cols = df.columns.filter(_ != label)
    var new_df = df
    if(needOneHot){
      for (col <- cols) {
        new_df = oneColProcessWithOneHot(col)(new_df)
        new_df.drop(col)
      }
    }else{
      for (col <- cols) {
        new_df = oneColProcess(col)(new_df)
        new_df.drop(col)
      }
    }
    new_df
  }

  //get train data
  def assembleFeatures(df:DataFrame, label:String):DataFrame = {
    val cols = df.columns
    val assembler = new VectorAssembler().
      setInputCols(cols.filter(_ != label)).
      setOutputCol("features")
    assembler.transform(df).select(label,"features")
  }

  //get imei data
  //tbname:user_profile.idl_fdt_dw_tag
  def getImeiData(tbname: String, sdate: Long, edate: Long): DataFrame = {
    /*
    val select_sql = "select user_age, sex, user_job,marriage_status,mz_apps_car_owner,user_network_type,user_life_city_lev," +
        "wifi_user_active,recharge_way_30d,mzpay_bind_bank_flag,dev_operator," +
        s"user_dev_price,user_os_type from $tbname"
    */
    val select_sql = "select user_age, sex, user_job,marriage_status,mz_apps_car_owner,user_network_type " +
      s"from $tbname"
    hiveContext.sql(select_sql)
  }

  //get app data
  //tbname: app_center.dim_application_info_d_all
  def getAppData(tbname: String, sdate: Long, edate: Long): DataFrame = {
    val select_sql = "select category_id,"
    hiveContext.sql(select_sql)
  }

  //get event data
  def getEventData(tbname: String, sdate: Long, edate: Long): DataFrame = {
    val select_sql = ""
    hiveContext.sql(select_sql)
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

}
