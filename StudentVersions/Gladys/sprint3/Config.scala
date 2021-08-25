package ca.mcit.bigdata.sprint3

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import java.sql.{Connection, DriverManager, Statement}


trait Config {
  //Hadoop
  val conf = new Configuration()

  val hadoopConfDir = "C:\\Users\\Vish\\Desktop\\hadoop"
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

  val fs:FileSystem = FileSystem.get(conf)

  val rootLogger: Logger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  //Hive
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)
  val connection: Connection = DriverManager.getConnection("jdbc:hive2://172.16.129.58:10000", "vish" ,"")
  val stmt: Statement = connection.createStatement()

}
