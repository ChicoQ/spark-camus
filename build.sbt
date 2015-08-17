name := "spark-camus"

version := "1.2.0"

scalaVersion := "2.10.4"

javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

scalacOptions += "-target:jvm-1.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.2.0" % "provided",
  "com.101tec" % "zkclient" % "0.4"
    exclude("org.apache.zookeeper", "zookeeper"),
  "org.apache.kafka" %% "kafka" % "0.8.1.1"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
    exclude("org.slf4j", "slf4j-simple")
    exclude("log4j", "log4j")
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("com.101tec", "zkclient")
)

resolvers += "AkkaRepository" at "http://repo.akka.io/releases/"

resolvers += "cloudera-repo-releases" at "https://repository.cloudera.com/artifactory/repo/"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.4.0" % "provided"