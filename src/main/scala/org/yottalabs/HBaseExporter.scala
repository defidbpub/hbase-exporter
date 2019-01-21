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
  private var selPropsFile = ""
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
        selPropsFile = config.argPropsFile
      case _ => println("Error")
    }

    val v_yaml_formated: List[(String, String)] = Parser.yamlParser(selPropsFile)
    val v_yaml_parsed: List[String] = v_yaml_formated.map(tuple => tuple._2)

    val sparkSession: SparkSession = SparkSession.builder
      .appName(v_yaml_parsed(1).replace("\"",""))
      .master(v_yaml_parsed.head.replace("\"",""))
      .getOrCreate

    val conf: Configuration = HBaseConfiguration.create()

    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", v_yaml_parsed(2).replace("\"",""))
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
                
    val table: Table = connection.getTable(TableName.valueOf(v_yaml_parsed(3).replace("\"","")))
    val result: util.Iterator[Result] = table.getScanner(scan).iterator()
    while (result.hasNext) {
      val data = result.next()
      loadRow(data)
      i += 1
    }
                                      
    val rdd: RDD[String] = sparkSession.sparkContext.makeRDD(allData)
                                        
    import sparkSession.implicits._
                         
    val full = rdd.toDF()
    full.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", v_yaml_parsed(4).replace("\"",""))
      .option("quote", "")
      .save(v_yaml_parsed(5).replace("\"",""))

    full.write.orc(v_yaml_parsed(6).replace("\"",""))

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("Row Count: " + i)
    println("Processing Time: " + durationSeconds.toString + " seconds")
    println("Done!")
  }

}