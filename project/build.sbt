logLevel := Level.Warn

resolvers += Classpaths.sbtPluginReleases
resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.4.4")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.0.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-twirl" % "1.1.1")
