name := "incognito"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0" withSources() withJavadoc()

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.0" withSources() withJavadoc()

libraryDependencies += "org.scala-lang.modules" % "scala-async_2.10" % "0.9.5" withSources() withJavadoc()
