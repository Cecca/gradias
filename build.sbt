import com.atlassian.labs.gitstamp.GitStampPlugin._
import org.eclipse.jgit.storage.file.FileRepositoryBuilder

lazy val commonSettings = Seq(
  organization  := "it.unipd.dei",
  version       := "0.11.0",
  scalaVersion  := "2.10.4",
  resolvers   += Resolver.mavenLocal,
  deploy := deployTaskImpl.value,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
    "org.apache.spark" %% "spark-graphx" % "1.4.0" % "provided",
    "org.rogach" %% "scallop" % "0.9.5",
    "it.unipd.dei" % "experiment-reporter" % "0.1.1",
    "it.unimi.dsi" % "fastutil" % "7.0.6",
    "it.unimi.dsi" % "webgraph" % "3.4.3"
      exclude("ch.qos.logback", "logback-classic")
      exclude("org.slf4j", "slf4j-api")
      exclude("it.unimi.dsi", "fastutil"),
    "org.scalatest" %% "scalatest" % "2.2.2" % "test"),
  scalacOptions := Seq(
    "-optimise",
    "-Xdisable-assertions",
    "-feature",
    "-deprecation",
    "-unchecked"),
  test in assembly := {})

lazy val gitSettings =
  if (new FileRepositoryBuilder().readEnvironment.findGitDir.getGitDir != null)
    gitStampSettings
  else
    Seq.empty

// custom tasks

lazy val deploy = Def.taskKey[Unit]("Deploy the jar")

lazy val deployTaskImpl = Def.task {
  val log = streams.value.log
  val account = "ceccarel@stargate.dei.unipd.it"
  val local = assembly.value.getPath
  val fname = assembly.value.getName
  val remote = s"$account:/mnt/gluster/ceccarel/lib/$fname"
  log.info(s"Deploy $fname to $remote")
  Seq("rsync", "--progress", "-z", local, remote) !
}

// projects

lazy val root = (project in file(".")).
  aggregate(core, benchmark)

lazy val core = (project in file("core")).
  settings(name := "gradias").
  settings(commonSettings :_*).
  settings(gitSettings :_*)

lazy val benchmark = (project in file("benchmark")).
  dependsOn(core).
  settings(
    name := "gradias-benchmark",
    libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.6").
  settings(commonSettings :_*)
