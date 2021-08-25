import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait HadoopClient {
  //set de Configuration
  val conf = new Configuration()
  var hadoopConfDir = "c:\\hadoop\\etc\\cloudera"
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

  // create the client of the filesystem
  val fs: FileSystem = FileSystem.get(conf)
}