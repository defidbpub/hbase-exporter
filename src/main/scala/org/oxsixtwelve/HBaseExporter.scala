package org.oxsixtwelve

/**
  * Created by Jan Rock on 02/03/2018.
  * Version: 0.1 (05/03/2018)
  */

import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object HBaseExporter {

  private var i: Int = 0
  private var allData: Array[String] = Array.empty

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()

    // TODO: add parameters prompt

//    case class Config(argZooIP: String = null, argAppName: String = null, argTable: String = null, argCFam: String = null, argCNum: String = null, argFilterVal: String = null, argExpPath: String = null)
//
//    val parser = new scopt.OptionParser[Config]("scopt") {
//      head("\nHBase-Exporter (Jan Rock)", "1.0.0")
//
//      opt[String]('z', "z") required() action { (x, c) =>
//        c.copy(argZooIP = x)
//      } text "flag for Zookeeper IP address"
//      opt[String]('n', "n") required() action { (x, c) =>
//        c.copy(argAppName = x)
//      } text "flag for Spark application name"
//      opt[String]('t', "t") required() action { (x, c) =>
//        c.copy(argTable = x)
//      } text "flag for HBase table to export"
//      opt[String]('f', "f") required() action { (x, c) =>
//        c.copy(argCFam = x)
//      } text "flag for HBase column family (filter)"
//      opt[String]('c', "c") required() action { (x, c) =>
//        c.copy(argCNum = x)
//      } text "flag for HBase column ID (filter)"
//      opt[String]('v', "v") required() action { (x, c) =>
//        c.copy(argFilterVal = x)
//      } text "flag for HBase column value (filter)"
//      opt[String]('e', "e") required() action { (x, c) =>
//        c.copy(argExpPath = x)
//      } text "flag for export <path>"
//    }

//    parser.parse(args, Config()) match {
//      case Some(config) =>
//        val selZooIP = config.argZooIP
//        val selAppName = config.argAppName
//        val selTable = config.argTable
//        val selCFam = config.argCFam
//        val selCNum = config.argCNum
//        val selFilterVal = config.argFilterVal
//        val selExpPath = config.argExpPath
//      case _ => println("Error")
//    }

    val sparkSession: SparkSession = SparkSession.builder
      .appName("hbaseexporter")
      .master("local[*]")
      .getOrCreate

    val conf: Configuration = HBaseConfiguration.create()

    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", "192.168.0.57")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.setInt("hbase.client.scanner.caching", 10000)

    val connection: Connection = ConnectionFactory.createConnection(conf)

    val scan = new Scan()
    val filter = new SingleColumnValueFilter(Bytes.toBytes("ca"),Bytes.toBytes("1"),CompareOp.EQUAL,Bytes.toBytes("version"))
    scan.setFilter(filter)
              
    def loadRow(result: Result): Unit = {
      val cells = result.rawCells()
      var dataRow: String = Bytes.toString(result.getRow)
      for (cell <- cells) {
        val colValue = "," + Bytes.toString(CellUtil.cloneValue(cell))
        dataRow = dataRow.concat(colValue)
      }
      allData :+= dataRow // to big array
    }
                
    val table: Table = connection.getTable(TableName.valueOf("config"))
    val result: util.Iterator[Result] = table.getScanner(scan).iterator()
    while (result.hasNext) {
      val data = result.next()
      loadRow(data)
      i += 1
    }
                                      
    val rdd: RDD[String] = sparkSession.sparkContext.makeRDD(allData)
                                        
    import sparkSession.implicits._
                         
    val full = rdd.toDF()                                        
    val df_final = full.select("value").as[String].map { value => value.split(",") }.map { case Array(key, attr, value) => (key, attr, value) }.toDF("key","attribute","value")
    
    df_final.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("ml-data.csv")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("Row Count: " + i)
    println("Processing Time: " + durationSeconds.toString)
    println("Done!")

  }

}
