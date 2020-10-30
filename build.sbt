name := "scalawithspark1"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++=Seq(
  "org.apache.spark"  %%  "spark-core"    % "3.0.0"   % "provided",
  "org.apache.spark"  %%  "spark-sql"     % "3.0.0",
  "org.apache.spark"  %%  "spark-mllib"   % "3.0.0"
)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}