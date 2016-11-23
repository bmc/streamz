name := "streamz-akka"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"          % Version.Akka,
  "com.typesafe.akka" %% "akka-stream"         % Version.Akka,
  "com.typesafe.akka" %% "akka-stream-testkit" % Version.Akka % "test"
)
