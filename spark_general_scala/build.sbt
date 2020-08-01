organization := "com.nitendratech"

name := "spark_general"

version := "1.0"

scalaVersion := "2.12.7"

libraryDependencies ++={
  val sparkV = "2.4.0"

  Seq (

    "org.apache.spark" %% "spark-streaming" % sparkV ,
    "org.apache.spark" %% "spark-core" % sparkV  ,
    "org.apache.spark" %% "spark-sql" % sparkV ,
    "io.delta" %% "delta-core" % "0.6.0" ,
    "log4j" % "log4j" % "1.2.17"
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", xs @ _*) => MergeStrategy.first
  case PathList("scala", xs @ _*) => MergeStrategy.discard

  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
