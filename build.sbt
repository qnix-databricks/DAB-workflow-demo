name := "workflow-demo"

version := "0.1.0"

scalaVersion := "2.13.16"

// Spark dependencies (provided scope since Databricks provides them)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql-api" % "4.0.1" % "provided",
  "com.databricks" %% "databricks-connect" % "17.0.2",
  "com.databricks" %% "databricks-dbutils-scala" % "0.1.5" % "provided",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0",

  // For compilation purposes only (not included in JAR)
  // "com.databricks" % "dbutils-api_2.12" % "0.0.5" % "provided",
  // Note: No 2.13 version found
  
  // Add other dependencies as needed
  "com.typesafe" % "config" % "1.4.3",
  
  // Apache Commons Math3 - needed for UDFs (SipCallsOneLeg Percentile calculation)
  "org.apache.commons" % "commons-math3" % "3.6.1"
)

// Assembly settings for creating fat JAR
assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

// Merge strategy for assembly
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case "reference.conf"              => MergeStrategy.concat
  case _                             => MergeStrategy.first
}

// Exclude Scala library from JAR (Databricks provides it)
// In sbt-assembly 2.x, use assemblyPackageScala instead of includeScala
assembly / assemblyPackageScala / assembleArtifact := false

// Exclude scala-library jar from the assembly
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp.filter(_.data.getName == "scala-library.jar")
}

// Scalastyle configuration - enforce code style rules
scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"
scalastyleFailOnError := true
scalastyleFailOnWarning := false

// Run scalastyle automatically on compile (optional - can be disabled if too strict)
// Uncomment the line below to check style on every compile:
// (Compile / compile) := ((Compile / compile) dependsOn (Compile / scalastyle)).value

// Compiler options to help catch issues
scalacOptions ++= Seq(
  "-deprecation",           // Emit warning and location for usages of deprecated APIs
  "-feature",               // Emit warning and location for usages of features that should be imported explicitly
  "-unchecked",             // Enable additional warnings where generated code depends on assumptions
  "-Xlint:unused",          // Warn about unused imports and other unused code
  "-Wunused:imports"        // Scala 2.13+ specific: warn about unused imports
  // "-Xfatal-warnings"     // Uncomment to turn warnings into errors (strict mode)
)

