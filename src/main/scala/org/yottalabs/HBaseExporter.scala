package org.yottalabs

/**
  * Created by Jan Rock on 18/01/2019.
  */

import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object HBaseExporter {

  private var i: Int = 0
  private var allData: Array[String] = Array.empty
  private val selPropsFile = ""
  private val props_master = ""
  private val props_appname = ""
  private val props_zookeeper = ""
  private val props_table = ""
  private val props_header = ""
  private val props_output = ""

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()

    case class Config(argPropsFile: String = null)

    val parser = new scopt.OptionParser[Config]("scopt") {
      head("\nHBase-Exporter (Jan Rock)", "1.0.0")

      opt[String]('p', "props") required() action { (x, c) =>
        c.copy(argPropsFile = x)
      } text "yaml configuration <path>"
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        val selPropsFile = config.argPropsFile
      case _ => println("Error")
    }

    val v_yaml_formated: List[(String, String)] = Parser.yamlParser(selPropsFile)
    val v_yaml_parsed: List[String] = v_yaml_formated.map(tuple => tuple._2)
    val v_stock_list: String = v_yaml_parsed.mkString(",")
    println(v_stock_list)
    sys.exit()

    val sparkSession: SparkSession = SparkSession.builder
      .appName("hbaseexporter")
      .master("local[*]")
      .getOrCreate

    val conf: Configuration = HBaseConfiguration.create()

    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", "uk01dl601")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.setInt("hbase.client.scanner.caching", 10000)

    val connection: Connection = ConnectionFactory.createConnection(conf)

    val scan = new Scan()

    def loadRow(result: Result): Unit = {
      val cells = result.rawCells()
      var dataRow: String = Bytes.toString(result.getRow)
      for (cell <- cells) {
        val colValue = "," + Bytes.toString(CellUtil.cloneValue(cell))
        dataRow = dataRow.concat(colValue)
      }
      allData :+= dataRow // to big array
    }
                
    val table: Table = connection.getTable(TableName.valueOf("MAKO:l0_term_ref"))
    val result: util.Iterator[Result] = table.getScanner(scan).iterator()
    while (result.hasNext) {
      val data = result.next()
      loadRow(data)
      i += 1
    }
                                      
    val rdd: RDD[String] = sparkSession.sparkContext.makeRDD(allData)
                                        
    import sparkSession.implicits._
                         
    val full = rdd.toDF()
    full.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("ml-data.csv")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("Row Count: " + i)
    println("Processing Time: " + durationSeconds.toString + " seconds")
    println("Done!")
  }

}