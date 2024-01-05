
name := "moesif-play-filter"
organization := "com.moesif.filter"

assemblyJarName in assembly := "moesif-play-filter-1.0.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// https://mvnrepository.com/artifact/com.moesif.api/moesifapi
libraryDependencies += "com.moesif.api" % "moesifapi" % "1.7.7-1"

// https://mvnrepository.com/artifact/com.typesafe.play/play
libraryDependencies += "com.typesafe.play" %% "play" % "2.6.23"

// dont use log4j lower than 2.17 due to Log4j for CVE-2021-45105
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.17.2"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.17.2"


assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filterNot  { f =>
    f.data.getName.contains("moesifapi")
  }
}

publishMavenStyle := true
licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/Moesif/moesif-play-filter"),
    "scm:git@github.com:Moesif/moesif-play-filter.git"
  )
)
organizationName := "Moesif Inc"
organizationHomepage := Some(url("http://www.moesif.com/"))
homepage := Some(url("https://github.com/Moesif/moesif-play-filter"))
developers += Developer("moesif", "Moesif API", "support@moesif.com", url("https://www.moesif.com"))
crossPaths := false

scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.13.1", "2.12.7", "2.11.8")
