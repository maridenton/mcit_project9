package ca.mcit.bigdata.hadoop

import org.apache.hive.jdbc.HiveDriver

import java.sql.{Connection, DriverManager, Statement}

trait hiveClient {

  Class.forName(classOf[HiveDriver].getName)

  val connectionString: String = "jdbc:hive2://quickstart.cloudera:10000/;user=marinda;"

  val connection: Connection = DriverManager.getConnection(connectionString)

  val stmt: Statement = connection.createStatement()

}