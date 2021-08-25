name := "Project"

version := "0.1"

scalaVersion := "2.11.8"

val hadoopVersion ="2.6.0"

libraryDependencies +="org.apache.hadoop"%"hadoop-common"% hadoopVersion
libraryDependencies +="org.apache.hadoop"%"hadoop-hdfs"% hadoopVersion
libraryDependencies += "com.github.agourlay" %% "json-2-csv" % "0.3.0"

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"
