
import java.io.File
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object SparkSQLHive {

  case class Record(lid: String, rid: String, voted: String)

  def main(args: Array[String]) {
    // When working with Hive, one must instantiate `SparkSession` with Hive support, including
    // connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined
    // functions. Users who do not have an existing Hive deployment can still enable Hive support.
    // When not configured by the hive-site.xml, the context automatically creates `metastore_db`
    // in the current directory and creates a directory configured by `spark.sql.warehouse.dir`,
    // which defaults to the directory `spark-warehouse` in the current directory that the spark
    // application is started.

    // $example on:spark_hive$
    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Comedy")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS comedy (lid STRING, rid STRING, voted STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    sql("LOAD DATA LOCAL INPATH '/home/cloudera/sparksql/comedy.train' OVERWRITE INTO TABLE comedy")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM comedy limit 10").show()

    sql("SELECT COUNT(*) FROM comedy").show()

    sql("select lid,count(*) as count from comedy group by lid order by count desc limit 10").show()

    sql("select rid,count(*) as count from comedy group by rid order by count desc limit 10").show()

    sql("select distinct lid,rid,voted from comedy where lid='W9y6nwBwwyQ' and voted='left' order by rid desc limit 10").show()

    sql("select distinct lid,rid,voted from comedy where rid='C8IJnUM0yQo' and voted='right' order by lid desc limit 10").show()

    spark.stop()
  }
}