package ad_search

import java.util.Calendar

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
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
  val featureMap = new mutable.HashMap[String, Map[String, Int]]()

  def initSpark(appname: String): Unit = {
    System.setProperty("user.name", "ad_recommend")
    System.setProperty("HADOOP_USER_NAME", "ad_recommend")
    val sparkConf: SparkConf = new SparkConf().setAppName(appname)
    sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    hiveContext = new HiveContext(sc)
    sqlContext = new SQLContext(sc)
  }

  def main(args: Array[String]): Unit = {
    initSpark("AdSearch-algo")
    val dt = args(0).toLong

    val imeiTable = "user_profile.idl_fdt_dw_tag"
    val appTable = "ad_recommend.bdl_fdt_app_application_ad"
    val eventTable = "ad_recommend.bdl_fdt_appcenter_ad_cpd_log"

    val outImeiTable = "ad_recommend.dxp_ad_search_imei_data"
    val outAppTable = "ad_recommend.dxp_ad_search_app_data"
    val outEventTable = "ad_recommend.dxp_ad_search_event_data"
    val ModuelFile = "ad_search_model"


    val imeiDF = getImeiData(imeiTable)
    saveDataFrame(imeiDF, outImeiTable, dt)
    val appDF =  getAppData(appTable,dt)
    saveDataFrame(appDF,outAppTable,dt)
    val eventDF = getEventData(eventTable,dt)
    saveDataFrame(eventDF, outEventTable,dt)

    val imeiCols = imeiDF.columns.filter(_ != "imei")
    val appCols = appDF.columns.filter(_ != "appid")
    val label = "oper_type"
    val eventCols = eventDF.columns.filter(!List("imei","appid",label).contains(_))
    val allCols = imeiCols ++ appCols ++ eventCols

    val mergeDF = eventDF.join(imeiDF,"imei").join(appDF, "appid").select(label, allCols:_*)

    //one hot

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
    df.withColumn(col, stringToDouble(df(col)))
  }

  /*
  def oneColProcessWithOneHot(col: String) = (df: DataFrame) => {
    val catMap = df.select(col).distinct.map(_.getAs[String](col).toString).collect.zipWithIndex.toMap
    println(catMap)

    val stringToVector =  udf[Vector, String] { w =>
      Vectors.sparse(catMap.size, Array(catMap(w)), Array(1))
    }
    val new_df = df.withColumn(col + "_cat", stringToVector(df(col)))
    new_df.drop(col)
  }
  */

  def convertToString(col: String) = (df: DataFrame) => {
    val sma = df.schema

    sma(col).dataType match {
      case StringType => df
      case _ => {
        val anyToString = sma(col).dataType match {
          case IntegerType => udf[String, Int] {
            _.toString
          }
          case LongType => udf[String, Long] {
            _.toString
          }
          case BooleanType => udf[String, Boolean] {
            _.toString
          }
          case DoubleType => udf[String, Double] {
            _.toString
          }
        }
        df.withColumn(col, anyToString(df(col)))
      }
    }
  }

  //对于非cat类型的字段进行分段处理
  def oneColProcessWithSplit(col: String, colRange: Array[Long]) = (df: DataFrame) => {
    //分段范围计算
    val splitToDouble = udf[Double, Double] { w =>
      var i = 0
      while (i < colRange.length && w > colRange(i)) {
        i += 1
      }
      i.toDouble
    }
    df.withColumn(col, splitToDouble(df(col)))
  }

  def multiColProcess(df: DataFrame, label: String): DataFrame = {
    val cols = df.columns.filter(_ != label)
    var new_df = df
    for (col <- cols) {
      new_df = convertToString(col)(new_df)
      new_df = oneColProcess(col)(new_df)
      println(new_df.columns)
    }
    new_df
  }

  //get train data
  def assembleFeatures(df: DataFrame, label: String): DataFrame = {
    val cols = df.columns
    val assembler = new VectorAssembler().
      setInputCols(cols.filter(_ != label)).
      setOutputCol("features")
    assembler.transform(df).select(label, "features")
  }

  def saveDataFrame(df: DataFrame, outTable: String, dt: Long): Unit = {
    val cols = df.columns
    val sma = df.schema
    val colsType = cols.map(r => {
      sma(r).dataType match {
        case IntegerType => "int"
        case LongType => "bigint"
        case StringType => "string"
        case BooleanType => "boolean"
        case DoubleType => "double"
      }
    })

    val colsString = cols.zip(colsType).map(r => r._1 + " " + r._2).mkString(",")
    val create_table_sql: String = s"create table if not exists $outTable " +
      s" ($colsString) partitioned by (stat_date bigint) stored as textfile"
    println(create_table_sql)
    hiveContext.sql(create_table_sql)

    val tmptable = "dxp_tmp_table"
    df.registerTempTable(tmptable)

    val insert_sql: String = s"insert overwrite table $outTable partition(stat_date = $dt) " +
      s"select * from $tmptable"
    hiveContext.sql(insert_sql)
    hiveContext.dropTempTable(tmptable)
  }

  //get imei data
  //tbname:user_profile.idl_fdt_dw_tag
  def getImeiData(tbname: String): DataFrame = {
    /*
    val cols = Array("user_age", "sex", "user_job", "marriage_status", "mz_apps_car_owner",
      "user_network_type","user_life_city_lev", "wifi_user_active","recharge_way_30d",
      "mzpay_bind_bank_flag","dev_operator")
      */
    val label = "imei"
    val cols = Array("user_age", "sex", "user_job", "marriage_status", "mz_apps_car_owner", "user_network_type")
    val colsString = label + "," + cols.mkString(",")
    val selectSQL = s"select $colsString from $tbname limit 10000"
    println(selectSQL)
    multiColProcess(hiveContext.sql(selectSQL), label)
  }

  //get app data
  def getAppData(tbname: String, dt: Long): DataFrame = {
    //tbname: bdl_fdt_app_application_ad
    val label = "appid"
    val catCols = Array("fcategoryid", "fcategory2id")
    val splitCols = Array("fevaluate_count", "fdownload_count", "fstars", "finstall_count", "appsize")
    val stdArray:Array[Long] = Array(1,10,100)
    val splitRanges: Array[Array[Long]] = Array(stdArray.map(_*10), stdArray.map(_*1000),
      stdArray.map(_*1000), stdArray.map(_*1000), Array(10240, 102400, 102400, 1024000))

    val colsString = (Array(label) ++ catCols ++ splitCols).mkString(",")
    val selectSQL = s"select $colsString from $tbname where stat_date=$dt"
    var df = hiveContext.sql(selectSQL)

    val splitColsRanges = splitCols.zip(splitRanges)
    for (col <- splitColsRanges) {
      df = oneColProcessWithSplit(col._1, col._2)(df)
    }
    multiColProcess(df,label)
  }

  //get event data
  def getEventData(tbname: String, dt: Long): DataFrame = {
    //tbname:bdl_fdt_appcenter_ad_cpd_log
    val cols = Array("imei", "app_id", "oper_type", "oper_time")
    val colsString = cols.mkString(",")

    val selectSQL = s"select $colsString from $tbname " +
      s" where stat_date=$dt and tracker_type = 2"

    val stringToDouble = udf[Double, String] { w =>
      w match {
        case "AD_MQ_EVENT_CPD_EXPOSE" => 0
        case "AD_MQ_EVENT_CPD_DETAIL" => 1
        case "AD_MQ_EVENT_CPD_INSTALL" => 1
        case _ => -1
      }
    }

    var cal = Calendar.getInstance()
    val getWeek = udf[Double, Long] { w =>
      cal.setTimeInMillis(w)
      cal.get(Calendar.DAY_OF_WEEK)
    }

    val getHour = udf[Double, Long] { w =>
      cal.setTimeInMillis(w)
      cal.get(Calendar.HOUR_OF_DAY) match {
        case i if List(2, 3, 4, 5, 6, 7).contains(i) => 0
        case i if List(8, 9, 10, 11, 12, 13).contains(i) => 1
        case i if List(14, 15, 16, 17, 18, 19).contains(i) => 2
        case i if List(20, 21, 22, 23, 0, 1).contains(i) => 3
        case _ => 4
      }
    }

    var df = hiveContext.sql(selectSQL)
    df = df.withColumn("oper_type", stringToDouble(df("oper_type")))

    df = df.withColumn("day_of_week", getWeek(df("oper_time")))
    df = df.withColumn("hour_of_day", getHour(df("oper_time")))
    df.drop("oper_time")
  }
}
