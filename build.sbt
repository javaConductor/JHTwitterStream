
name := "JHTwitter"
version := "1.0"
scalaVersion := "2.11.8"


fork := true
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies += "com.twitter" % "hbc-core" % "2.2.0"
libraryDependencies += "net.oauth.core" % "oauth" % "20100527"
libraryDependencies += "org.json4s" % "json4s-jackson_2.11" % "3.4.0"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21"

//test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
