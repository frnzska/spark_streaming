name := "spark_streaming"
version := "0.1"
scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0"
