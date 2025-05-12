name := "RunLSA"
version := "1.0"

// Set the Scala version compatible with your Spark version on IRIS
// e.g., Spark 3.5.1 often uses Scala 2.12.x or 2.13.x
// Check your IRIS Spark module for the Scala version it was built with.
// For Spark 3.5.1 (Java 17) from your module, Scala 2.12.17 or 2.13.8 might be appropriate.
// Let's assume Scala 2.12.17 for this example.
scalaVersion := "2.12.17"

// Spark Dependencies (use versions compatible with your IRIS cluster Spark)
// Your module: devel/Spark/3.5.1-foss-2023b-Java-17
val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
)

// Stanford CoreNLP Dependencies
// Ensure this version is compatible with your Java version (Java 17 from your module)
// Stanford CoreNLP 4.5.x versions should generally be fine with Java 11+
val stanfordNLPVersion = "4.5.6" // Use a recent, stable version

libraryDependencies ++= Seq(
  "edu.stanford.nlp" % "stanford-corenlp" % stanfordNLPVersion,
  "edu.stanford.nlp" % "stanford-corenlp" % stanfordNLPVersion classifier "models" // To include the models
)

// sbt-assembly plugin settings for creating a fat JAR
// This helps package dependencies like Stanford CoreNLP.
// Spark JARs are excluded because they are marked "provided".
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard // Discard duplicate META-INF files
  case "reference.conf"             => MergeStrategy.concat  // Concatenate reference.conf files
  case x => MergeStrategy.first                               // For others, take the first one found
}

// Optional: Specify the main class for the assembly JAR
// This allows `java -jar YourJar.jar` but is not strictly needed for `spark-submit --class ...`
// assembly / mainClass := Some("RunLSA")
