package sprint2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait hClient {
  val conf = new Configuration()
  val hadoopConfDir = "data/ClientXmlFile"
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

  //Create the Client
  val fs = FileSystem.get(conf)
}
