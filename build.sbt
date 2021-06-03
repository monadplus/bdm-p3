val scala2Version = "2.12.13"
val scala3Version = "3.0.0"
val circeVersion = "0.13.0"
val munitVersion = "0.7.20"
val sparkVersion = "3.1.2"
val catsVersion = "2.6.0"
val betterMonadicForVersion = "0.3.1"
val kindProjectorVersion = "0.13.0"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    organization := "com.example",
    name := "bdm",
    version := "0.0.1",
    scalaVersion := scala2Version,
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core"
    ).map(_ % catsVersion) ++ Seq(
      "io.circe" %% "circe-core",
      "io.circe" %% "circe-generic",
      "io.circe" %% "circe-parser"
    ).map(_ % circeVersion) ++ Seq(
      "spark-core",
      "spark-sql",
      "spark-mllib",
      "spark-streaming",
      "spark-sql-kafka-0-10",
      "spark-streaming-kafka-0-10"
    ).map("org.apache.spark" %% _ % sparkVersion) ++ Seq(
      "org.scalameta" %% "munit" % munitVersion % Test
    ),
    resolvers ++= Seq(
      "sonatype-releases".at("https://oss.sonatype.org/content/repositories/releases/"),
      "Typesafe repository".at("https://repo.typesafe.com/typesafe/releases/"),
      "Second Typesafe repo".at("https://repo.typesafe.com/typesafe/maven-releases/"),
      Resolver.sonatypeRepo("public")
    ),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForVersion),
    addCompilerPlugin(("org.typelevel" %% "kind-projector" % kindProjectorVersion).cross(CrossVersion.full)),
    testFrameworks += new TestFramework("munit.Framework")
  )
