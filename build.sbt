name := "spark-t-digest"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.1.1" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.apache.spark" %% "spark-catalyst" % "2.1.1",
  "com.tdunning" % "t-digest" % "3.1"
)