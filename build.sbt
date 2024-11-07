lazy val root = (project in file(".")).settings(
  name         := "skunk-hackday",
  organization := "nad.emil",
  scalaVersion := "3.5.2",
  libraryDependencies += "org.tpolecat" %% "skunk-core" % "1.1.0-M3"
)
