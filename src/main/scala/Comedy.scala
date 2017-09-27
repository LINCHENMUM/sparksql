import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
object Comedy {
  private val schemaString = "lid,rid,voted"
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Exercise:Comedy Data Statistics").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val comedyDataRDD = sc.textFile("/home/cloudera/sparksql/comedy.train")

    val sqlCtx = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlCtx.implicits._
    val schemaArray = schemaString.split(",")
    val schema = StructType(schemaArray.map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD: RDD[Row] = comedyDataRDD.map(_.split(",")).map(
      eachRow => Row(eachRow(0), eachRow(1), eachRow(2)))
    val comedyDF = sqlCtx.createDataFrame(rowRDD, schema)
    comedyDF.registerTempTable("comedy")
    //Begin
    println("<Display #1 get total rows>")
    val recordAmount = sqlCtx.sql("select * from comedy")
    println("The total records in comedy table: " + recordAmount.count())
    //The total records in comedy table: 912969                    

    println("<Display #2 get top 10 group by voted>")

    val grpbyvoted = sqlCtx.sql("select voted,count(*) as count from comedy group by voted")
    grpbyvoted.map(r => "Voted: " + r(0) + "       Count:" + r(1)).collect().foreach(println)
    /*Voted: left       Count:440364
Voted: right       Count:472605*/

    println("<Display #3  The top 10 left highest voted comedies>")
    //Grouped the comedy by gender and count the number
    val grpbyleftvoted = sqlCtx.sql("select lid,count(*) as count from comedy group by lid order by count desc limit 10")
    grpbyleftvoted.map(r => "lid: " + r(0) + "       Count:" + r(1)).collect().foreach(println)
    /*lid: C8IJnUM0yQo       Count:29320
lid: W9y6nwBwwyQ       Count:25262
lid: LLaKkC5U9Po       Count:24628
lid: 7zCIRPQ8qWc       Count:20775
lid: bOcugYjrvLg       Count:18695
lid: u2-od4n5Xl0       Count:18588
lid: YowPM7yZv2U       Count:18374
lid: 0MD6Cx0qzRA       Count:17812
lid: XIPhUZZBtsc       Count:17627
lid: HRaEcDT3uqw       Count:15922*/

    println("<Display #4  The top 10 highest righ voted comedies>")
    //Grouped the comedy by gender and count the number
    val grpbyrightvoted = sqlCtx.sql("select rid,count(*) as count from comedy group by rid order by count desc limit 10")
    grpbyrightvoted.map(r => "rid: " + r(0) + "       Count:" + r(1)).collect().foreach(println)

    /*rid: C8IJnUM0yQo       Count:29186
rid: W9y6nwBwwyQ       Count:25704
rid: LLaKkC5U9Po       Count:24666
rid: 7zCIRPQ8qWc       Count:21019
rid: u2-od4n5Xl0       Count:18811
rid: YowPM7yZv2U       Count:18777
rid: bOcugYjrvLg       Count:18494
rid: XIPhUZZBtsc       Count:17824
rid: 0MD6Cx0qzRA       Count:17661
rid: HRaEcDT3uqw       Count:16027*/

    println("<Display #5 The 10 comedies in right are defeated by the toppest left comedy W9y6nwBwwyQ>")
    val listrightvoted = sqlCtx.sql("select distinct lid,rid,voted from comedy where lid='W9y6nwBwwyQ' and voted='left' order by rid desc limit 10")
    listrightvoted.map(r => "lid:" + r(0) + "    rid: " + r(1) + "       voted:" + r(2)).collect().foreach(println)

    /*lid:W9y6nwBwwyQ    rid: xv3TwkmOpbk       voted:left
lid:W9y6nwBwwyQ    rid: xfPLiMbfOmY       voted:left
lid:W9y6nwBwwyQ    rid: xBSYD0dQCAw       voted:left
lid:W9y6nwBwwyQ    rid: wpcD2BV6abc       voted:left
lid:W9y6nwBwwyQ    rid: wDx28Y2RcCI       voted:left
lid:W9y6nwBwwyQ    rid: uvOandCCRFg       voted:left
lid:W9y6nwBwwyQ    rid: u2-od4n5Xl0       voted:left
lid:W9y6nwBwwyQ    rid: tG6CujNS6J0       voted:left
lid:W9y6nwBwwyQ    rid: t4KvXsglvMg       voted:left
lid:W9y6nwBwwyQ    rid: rizNup9BWhU       voted:left*/

    println("<Display #6 The 10 comedies in lid are defeated by the toppest right comedy C8IJnUM0yQo>")
    val listleftvoted = sqlCtx.sql("select distinct lid,rid,voted from comedy where rid='C8IJnUM0yQo' and voted='right' order by lid desc limit 10")
    listleftvoted.map(r => "lid:" + r(0) + "    rid: " + r(1) + "       voted:" + r(2)).collect().foreach(println)
    /*
lid:zKAW96N-Vms    rid: C8IJnUM0yQo       voted:right
lid:xfPLiMbfOmY    rid: C8IJnUM0yQo       voted:right
lid:xWWNG0JBMCg    rid: C8IJnUM0yQo       voted:right
lid:xQUZSnZ6R3E    rid: C8IJnUM0yQo       voted:right
lid:wvdWSsSqgLc    rid: C8IJnUM0yQo       voted:right
lid:wXFRLvzOEgU    rid: C8IJnUM0yQo       voted:right
lid:wTlaRunPV9Q    rid: C8IJnUM0yQo       voted:right
lid:wDx28Y2RcCI    rid: C8IJnUM0yQo       voted:right
lid:w8UWgufUIv0    rid: C8IJnUM0yQo       voted:right
lid:vqm_vNS-HX0    rid: C8IJnUM0yQo       voted:right*/

    println("<Display #7 Compare the top 10 left comedies to the top right comedies>")
    //
    //val llist = List("C8IJnUM0yQo","W9y6nwBwwyQ","LLaKkC5U9Po","7zCIRPQ8qWc","bOcugYjrvLg","u2-od4n5Xl0","YowPM7yZv2U","0MD6Cx0qzRA","XIPhUZZBtsc","HRaEcDT3uqw")
    //val rlist = List("C8IJnUM0yQo","W9y6nwBwwyQ","LLaKkC5U9Po","7zCIRPQ8qWc","u2-od4n5Xl0","YowPM7yZv2U","bOcugYjrvLg","XIPhUZZBtsc","0MD6Cx0qzRA","HRaEcDT3uqw")

    val leftrightvoted = sqlCtx.sql("select distinct lid,rid,voted from comedy where (rid='C8IJnUM0yQo' or rid='W9y6nwBwwyQ' or rid='LLaKkC5U9Po' or rid='7zCIRPQ8qWc' or rid='u2-od4n5Xl0' or rid='YowPM7yZv2U' or rid='bOcugYjrvLg' or rid= 'XIPhUZZBtsc' or rid='0MD6Cx0qzRA' or rid='HRaEcDT3uqw')  and (lid= 'C8IJnUM0yQo' or lid='W9y6nwBwwyQ'  or lid='LLaKkC5U9Po' or lid='7zCIRPQ8qWc' or lid ='bOcugYjrvLg' or lid='u2-od4n5Xl0' or lid='YowPM7yZv2U'  or lid='0MD6Cx0qzRA'  or lid='XIPhUZZBtsc'  or lid='HRaEcDT3uqw') order by lid desc,rid desc")

    //val leftrightvoted = sqlCtx.sql("select distinct lid,rid,voted from comedy where rid in (${rlist.map ( x => ''' + x + ''').mkString(',') }) and lid (${llist.map ( x => ''' + x + ''').mkString(',') })")
    leftrightvoted.map(r => "lid:" + r(0) + "    rid: " + r(1) + "       voted:" + r(2)).collect().foreach(println)

    /*lid:u2-od4n5Xl0    rid: bOcugYjrvLg       voted:left
lid:u2-od4n5Xl0    rid: bOcugYjrvLg       voted:right
lid:u2-od4n5Xl0    rid: YowPM7yZv2U       voted:left
lid:u2-od4n5Xl0    rid: YowPM7yZv2U       voted:right
lid:u2-od4n5Xl0    rid: XIPhUZZBtsc       voted:left
lid:u2-od4n5Xl0    rid: XIPhUZZBtsc       voted:right
lid:u2-od4n5Xl0    rid: W9y6nwBwwyQ       voted:left
lid:u2-od4n5Xl0    rid: W9y6nwBwwyQ       voted:right
lid:u2-od4n5Xl0    rid: LLaKkC5U9Po       voted:right
lid:u2-od4n5Xl0    rid: LLaKkC5U9Po       voted:left
lid:u2-od4n5Xl0    rid: HRaEcDT3uqw       voted:left
lid:u2-od4n5Xl0    rid: HRaEcDT3uqw       voted:right
lid:u2-od4n5Xl0    rid: C8IJnUM0yQo       voted:left
lid:u2-od4n5Xl0    rid: C8IJnUM0yQo       voted:right
lid:u2-od4n5Xl0    rid: 7zCIRPQ8qWc       voted:right
lid:u2-od4n5Xl0    rid: 7zCIRPQ8qWc       voted:left
lid:u2-od4n5Xl0    rid: 0MD6Cx0qzRA       voted:left
lid:u2-od4n5Xl0    rid: 0MD6Cx0qzRA       voted:right
lid:bOcugYjrvLg    rid: u2-od4n5Xl0       voted:left
lid:bOcugYjrvLg    rid: u2-od4n5Xl0       voted:right
lid:bOcugYjrvLg    rid: YowPM7yZv2U       voted:left
lid:bOcugYjrvLg    rid: YowPM7yZv2U       voted:right
lid:bOcugYjrvLg    rid: XIPhUZZBtsc       voted:right
lid:bOcugYjrvLg    rid: XIPhUZZBtsc       voted:left
lid:bOcugYjrvLg    rid: W9y6nwBwwyQ       voted:left
lid:bOcugYjrvLg    rid: W9y6nwBwwyQ       voted:right
lid:bOcugYjrvLg    rid: LLaKkC5U9Po       voted:left
lid:bOcugYjrvLg    rid: LLaKkC5U9Po       voted:right
lid:bOcugYjrvLg    rid: HRaEcDT3uqw       voted:left
lid:bOcugYjrvLg    rid: HRaEcDT3uqw       voted:right
lid:bOcugYjrvLg    rid: C8IJnUM0yQo       voted:left
lid:bOcugYjrvLg    rid: C8IJnUM0yQo       voted:right
lid:bOcugYjrvLg    rid: 7zCIRPQ8qWc       voted:left
lid:bOcugYjrvLg    rid: 7zCIRPQ8qWc       voted:right
lid:bOcugYjrvLg    rid: 0MD6Cx0qzRA       voted:left
lid:bOcugYjrvLg    rid: 0MD6Cx0qzRA       voted:right
lid:YowPM7yZv2U    rid: u2-od4n5Xl0       voted:right
lid:YowPM7yZv2U    rid: u2-od4n5Xl0       voted:left
lid:YowPM7yZv2U    rid: bOcugYjrvLg       voted:left
lid:YowPM7yZv2U    rid: bOcugYjrvLg       voted:right
lid:YowPM7yZv2U    rid: XIPhUZZBtsc       voted:left
lid:YowPM7yZv2U    rid: XIPhUZZBtsc       voted:right
lid:YowPM7yZv2U    rid: W9y6nwBwwyQ       voted:right
lid:YowPM7yZv2U    rid: W9y6nwBwwyQ       voted:left
lid:YowPM7yZv2U    rid: LLaKkC5U9Po       voted:right
lid:YowPM7yZv2U    rid: LLaKkC5U9Po       voted:left
lid:YowPM7yZv2U    rid: C8IJnUM0yQo       voted:right
lid:YowPM7yZv2U    rid: C8IJnUM0yQo       voted:left
lid:YowPM7yZv2U    rid: 7zCIRPQ8qWc       voted:right
lid:YowPM7yZv2U    rid: 7zCIRPQ8qWc       voted:left
lid:YowPM7yZv2U    rid: 0MD6Cx0qzRA       voted:left
lid:YowPM7yZv2U    rid: 0MD6Cx0qzRA       voted:right
lid:XIPhUZZBtsc    rid: u2-od4n5Xl0       voted:right
lid:XIPhUZZBtsc    rid: u2-od4n5Xl0       voted:left
lid:XIPhUZZBtsc    rid: bOcugYjrvLg       voted:left
lid:XIPhUZZBtsc    rid: bOcugYjrvLg       voted:right
lid:XIPhUZZBtsc    rid: YowPM7yZv2U       voted:right
lid:XIPhUZZBtsc    rid: YowPM7yZv2U       voted:left
lid:XIPhUZZBtsc    rid: W9y6nwBwwyQ       voted:right
lid:XIPhUZZBtsc    rid: W9y6nwBwwyQ       voted:left
lid:XIPhUZZBtsc    rid: LLaKkC5U9Po       voted:left
lid:XIPhUZZBtsc    rid: LLaKkC5U9Po       voted:right
lid:XIPhUZZBtsc    rid: HRaEcDT3uqw       voted:left
lid:XIPhUZZBtsc    rid: HRaEcDT3uqw       voted:right
lid:XIPhUZZBtsc    rid: C8IJnUM0yQo       voted:left
lid:XIPhUZZBtsc    rid: C8IJnUM0yQo       voted:right
lid:XIPhUZZBtsc    rid: 7zCIRPQ8qWc       voted:left
lid:XIPhUZZBtsc    rid: 7zCIRPQ8qWc       voted:right
lid:XIPhUZZBtsc    rid: 0MD6Cx0qzRA       voted:right
lid:XIPhUZZBtsc    rid: 0MD6Cx0qzRA       voted:left
lid:W9y6nwBwwyQ    rid: u2-od4n5Xl0       voted:left
lid:W9y6nwBwwyQ    rid: u2-od4n5Xl0       voted:right
lid:W9y6nwBwwyQ    rid: bOcugYjrvLg       voted:left
lid:W9y6nwBwwyQ    rid: bOcugYjrvLg       voted:right
lid:W9y6nwBwwyQ    rid: YowPM7yZv2U       voted:right
lid:W9y6nwBwwyQ    rid: YowPM7yZv2U       voted:left
lid:W9y6nwBwwyQ    rid: XIPhUZZBtsc       voted:right
lid:W9y6nwBwwyQ    rid: XIPhUZZBtsc       voted:left
lid:W9y6nwBwwyQ    rid: LLaKkC5U9Po       voted:right
lid:W9y6nwBwwyQ    rid: LLaKkC5U9Po       voted:left
lid:W9y6nwBwwyQ    rid: HRaEcDT3uqw       voted:right
lid:W9y6nwBwwyQ    rid: HRaEcDT3uqw       voted:left
lid:W9y6nwBwwyQ    rid: C8IJnUM0yQo       voted:right
lid:W9y6nwBwwyQ    rid: C8IJnUM0yQo       voted:left
lid:W9y6nwBwwyQ    rid: 7zCIRPQ8qWc       voted:left
lid:W9y6nwBwwyQ    rid: 7zCIRPQ8qWc       voted:right
lid:W9y6nwBwwyQ    rid: 0MD6Cx0qzRA       voted:left
lid:W9y6nwBwwyQ    rid: 0MD6Cx0qzRA       voted:right
lid:LLaKkC5U9Po    rid: u2-od4n5Xl0       voted:left
lid:LLaKkC5U9Po    rid: u2-od4n5Xl0       voted:right
lid:LLaKkC5U9Po    rid: bOcugYjrvLg       voted:right
lid:LLaKkC5U9Po    rid: bOcugYjrvLg       voted:left
lid:LLaKkC5U9Po    rid: YowPM7yZv2U       voted:left
lid:LLaKkC5U9Po    rid: YowPM7yZv2U       voted:right
lid:LLaKkC5U9Po    rid: XIPhUZZBtsc       voted:left
lid:LLaKkC5U9Po    rid: XIPhUZZBtsc       voted:right
lid:LLaKkC5U9Po    rid: W9y6nwBwwyQ       voted:left
lid:LLaKkC5U9Po    rid: W9y6nwBwwyQ       voted:right
lid:LLaKkC5U9Po    rid: HRaEcDT3uqw       voted:left
lid:LLaKkC5U9Po    rid: HRaEcDT3uqw       voted:right
lid:LLaKkC5U9Po    rid: C8IJnUM0yQo       voted:left
lid:LLaKkC5U9Po    rid: C8IJnUM0yQo       voted:right
lid:LLaKkC5U9Po    rid: 7zCIRPQ8qWc       voted:left
lid:LLaKkC5U9Po    rid: 7zCIRPQ8qWc       voted:right
lid:LLaKkC5U9Po    rid: 0MD6Cx0qzRA       voted:right
lid:LLaKkC5U9Po    rid: 0MD6Cx0qzRA       voted:left
lid:HRaEcDT3uqw    rid: u2-od4n5Xl0       voted:right
lid:HRaEcDT3uqw    rid: u2-od4n5Xl0       voted:left
lid:HRaEcDT3uqw    rid: bOcugYjrvLg       voted:left
lid:HRaEcDT3uqw    rid: bOcugYjrvLg       voted:right
lid:HRaEcDT3uqw    rid: YowPM7yZv2U       voted:right
lid:HRaEcDT3uqw    rid: XIPhUZZBtsc       voted:right
lid:HRaEcDT3uqw    rid: XIPhUZZBtsc       voted:left
lid:HRaEcDT3uqw    rid: W9y6nwBwwyQ       voted:right
lid:HRaEcDT3uqw    rid: W9y6nwBwwyQ       voted:left
lid:HRaEcDT3uqw    rid: LLaKkC5U9Po       voted:right
lid:HRaEcDT3uqw    rid: LLaKkC5U9Po       voted:left
lid:HRaEcDT3uqw    rid: C8IJnUM0yQo       voted:right
lid:HRaEcDT3uqw    rid: C8IJnUM0yQo       voted:left
lid:HRaEcDT3uqw    rid: 7zCIRPQ8qWc       voted:right
lid:HRaEcDT3uqw    rid: 7zCIRPQ8qWc       voted:left
lid:HRaEcDT3uqw    rid: 0MD6Cx0qzRA       voted:left
lid:HRaEcDT3uqw    rid: 0MD6Cx0qzRA       voted:right
lid:C8IJnUM0yQo    rid: u2-od4n5Xl0       voted:right
lid:C8IJnUM0yQo    rid: u2-od4n5Xl0       voted:left
lid:C8IJnUM0yQo    rid: bOcugYjrvLg       voted:left
lid:C8IJnUM0yQo    rid: bOcugYjrvLg       voted:right
lid:C8IJnUM0yQo    rid: YowPM7yZv2U       voted:right
lid:C8IJnUM0yQo    rid: YowPM7yZv2U       voted:left
lid:C8IJnUM0yQo    rid: XIPhUZZBtsc       voted:left
lid:C8IJnUM0yQo    rid: XIPhUZZBtsc       voted:right
lid:C8IJnUM0yQo    rid: W9y6nwBwwyQ       voted:left
lid:C8IJnUM0yQo    rid: W9y6nwBwwyQ       voted:right
lid:C8IJnUM0yQo    rid: LLaKkC5U9Po       voted:left
lid:C8IJnUM0yQo    rid: LLaKkC5U9Po       voted:right
lid:C8IJnUM0yQo    rid: HRaEcDT3uqw       voted:left
lid:C8IJnUM0yQo    rid: HRaEcDT3uqw       voted:right
lid:C8IJnUM0yQo    rid: 7zCIRPQ8qWc       voted:right
lid:C8IJnUM0yQo    rid: 7zCIRPQ8qWc       voted:left
lid:C8IJnUM0yQo    rid: 0MD6Cx0qzRA       voted:right
lid:C8IJnUM0yQo    rid: 0MD6Cx0qzRA       voted:left
lid:7zCIRPQ8qWc    rid: u2-od4n5Xl0       voted:left
lid:7zCIRPQ8qWc    rid: u2-od4n5Xl0       voted:right
lid:7zCIRPQ8qWc    rid: bOcugYjrvLg       voted:left
lid:7zCIRPQ8qWc    rid: bOcugYjrvLg       voted:right
lid:7zCIRPQ8qWc    rid: YowPM7yZv2U       voted:left
lid:7zCIRPQ8qWc    rid: YowPM7yZv2U       voted:right
lid:7zCIRPQ8qWc    rid: XIPhUZZBtsc       voted:left
lid:7zCIRPQ8qWc    rid: XIPhUZZBtsc       voted:right
lid:7zCIRPQ8qWc    rid: W9y6nwBwwyQ       voted:left
lid:7zCIRPQ8qWc    rid: W9y6nwBwwyQ       voted:right
lid:7zCIRPQ8qWc    rid: LLaKkC5U9Po       voted:right
lid:7zCIRPQ8qWc    rid: LLaKkC5U9Po       voted:left
lid:7zCIRPQ8qWc    rid: HRaEcDT3uqw       voted:left
lid:7zCIRPQ8qWc    rid: HRaEcDT3uqw       voted:right
lid:7zCIRPQ8qWc    rid: C8IJnUM0yQo       voted:left
lid:7zCIRPQ8qWc    rid: C8IJnUM0yQo       voted:right
lid:7zCIRPQ8qWc    rid: 0MD6Cx0qzRA       voted:left
lid:7zCIRPQ8qWc    rid: 0MD6Cx0qzRA       voted:right
lid:0MD6Cx0qzRA    rid: u2-od4n5Xl0       voted:left
lid:0MD6Cx0qzRA    rid: u2-od4n5Xl0       voted:right
lid:0MD6Cx0qzRA    rid: bOcugYjrvLg       voted:left
lid:0MD6Cx0qzRA    rid: bOcugYjrvLg       voted:right
lid:0MD6Cx0qzRA    rid: YowPM7yZv2U       voted:right
lid:0MD6Cx0qzRA    rid: YowPM7yZv2U       voted:left
lid:0MD6Cx0qzRA    rid: XIPhUZZBtsc       voted:left
lid:0MD6Cx0qzRA    rid: XIPhUZZBtsc       voted:right
lid:0MD6Cx0qzRA    rid: W9y6nwBwwyQ       voted:right
lid:0MD6Cx0qzRA    rid: W9y6nwBwwyQ       voted:left
lid:0MD6Cx0qzRA    rid: LLaKkC5U9Po       voted:right
lid:0MD6Cx0qzRA    rid: LLaKkC5U9Po       voted:left
lid:0MD6Cx0qzRA    rid: HRaEcDT3uqw       voted:left
lid:0MD6Cx0qzRA    rid: HRaEcDT3uqw       voted:right
lid:0MD6Cx0qzRA    rid: C8IJnUM0yQo       voted:left
lid:0MD6Cx0qzRA    rid: C8IJnUM0yQo       voted:right
lid:0MD6Cx0qzRA    rid: 7zCIRPQ8qWc       voted:right
lid:0MD6Cx0qzRA    rid: 7zCIRPQ8qWc       voted:left*/

    //End......
    println("All the statistics actions are finished on structured comedy data.")

  }
}