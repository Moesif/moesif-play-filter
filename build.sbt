
name := "moesif-play-filter"
organization := "com.moesif.filter"
ThisBuild / resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "moesif-play-filter-1.0.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// https://mvnrepository.com/artifact/com.moesif.api/moesifapi
libraryDependencies += "com.moesif.api" % "moesifapi" % "1.8.5"

// https://mvnrepository.com/artifact/com.typesafe.play/play
libraryDependencies += "com.typesafe.play" %% "play" % "2.6.23"

// dont use log4j lower than 2.17 due to Log4j for CVE-2021-45105
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.17.2"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.17.2"

libraryDependencies ++= Seq(
  "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.3" % Test,
  "org.mockito" % "mockito-core" % "2.28.2" % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
  "org.slf4j" % "slf4j-api" % "1.7.30" % Test
)

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

scalaVersion := "2.12.20"
crossScalaVersions := Seq("2.12.20", "2.11.8")

Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
