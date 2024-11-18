lazy val root = (project in file(".")).settings(
  name         := "skunk-hackday",
  organization := "nad.emil",
  scalaVersion := "2.13.12",
  libraryDependencies += "org.tpolecat" %% "skunk-core" % "1.1.0-M3",
  libraryDependencies += "org.postgresql" % "postgresql" % "42.3.1",
  libraryDependencies += "com.zaxxer" % "HikariCP" % "3.4.5",
  libraryDependencies += "org.playframework.anorm" %% "anorm" % "2.6.10"
)
