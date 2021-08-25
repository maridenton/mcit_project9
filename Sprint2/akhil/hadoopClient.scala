package ca.mcit.bigdata.hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait hadoopClient {
    val Conf = new Configuration()
    val hadoopConfDir = "/Users/marinda/opt/hadoop/etc/cloudera"
    Conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
    Conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

    val fs: FileSystem = FileSystem.get(Conf)

  }
