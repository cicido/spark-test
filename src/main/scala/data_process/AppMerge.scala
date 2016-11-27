package data_process

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object AppMerge{
  var hiveContext: HiveContext = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null

  def main(args: Array[String]): Unit = {
    initSpark("AppData-Merge")
    val dt = args(0).toLong
    val adTable = "ad_recommend.bdl_t_poster"
    val appTable = "app_center.bdl_fdt_app_application"
    val verTable = "app_center.bdl_fdt_t_app_version"
    val srcTables = Map("ad"->adTable,"app"->appTable,"ver"->verTable)

    val dstTable = "ad_recommend.bdl_fdt_app_application_ad"

    mergeData(srcTables,dt,dstTable)
  }

  def initSpark(appname: String): Unit = {
    System.setProperty("user.name", "ad_recommend")
    System.setProperty("HADOOP_USER_NAME", "ad_recommend")
    val sparkConf: SparkConf = new SparkConf().setAppName(appname)
    sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    hiveContext = new HiveContext(sc)
    sqlContext = new SQLContext(sc)
  }

  def getData(tbname:String,dt:BigInt): DataFrame = {
    hiveContext.sql(buildSQL(tbname,dt))
  }

  def buildSQL(tbname:String, dt:BigInt): String ={
    val cols = Array("fid","fname","fcategoryid","fcategory2id","fstatus",
      "fevaluate_count","fdownload_count","fstars","finstall_count")
    val cols_string = cols.mkString(",")

    tbname match {
      case "ad_recommend.bdl_t_poster" => s"select distinct(fappid) as appid from $tbname where stat_date =$dt"
      case "app_center.bdl_fdt_app_application" => s"select $cols_string from $tbname where stat_date =$dt"
      case "app_center.bdl_fdt_t_app_version" => s"select fappid as appid,max(fsize) as appsize" +
        s" from $tbname where stat_date =$dt group by fappid"
    }
  }

  def mergeData(srcTables:Map[String,String], dt:BigInt,outTable:String): Unit ={
    val ad_df:DataFrame = getData(srcTables("ad"), dt)
    val app_df:DataFrame = getData(srcTables("app"),dt)
    val ver_df:DataFrame = getData(srcTables("ver"),dt)


    val cols = ad_df.columns ++ app_df.columns.filter( _ != "fid") ++
      ver_df.columns.filter(_ != "appid")

    val tmptable = "dxp_tmp_table"
    val df = ad_df.join(app_df,ad_df("appid") === app_df("fid")).join(ver_df,"appid").selectExpr(cols:_*)
    df.registerTempTable(tmptable)
    val sma = df.schema
    val colsType = cols.map(r=>{
      sma(r).dataType match {
        case IntegerType => "int"
        case LongType => "bigint"
        case StringType => "string"
      }
    })

    val colsString = cols.zip(colsType).map(r=>r._1 + " " + r._2).mkString(",")
    val create_table_sql: String = s"create table if not exists $outTable " +
      s" ($colsString) partitioned by (stat_date bigint) stored as textfile"
    hiveContext.sql(create_table_sql)

    val insert_sql: String = s"insert overwrite table $outTable partition(stat_date = $dt) " +
      s"select * from $tmptable"
    hiveContext.sql(insert_sql)
  }
}


