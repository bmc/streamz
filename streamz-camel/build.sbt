name := "streamz-camel"

libraryDependencies ++= Seq(
  "org.apache.camel"   % "camel-core"  % Version.Camel,
  "com.typesafe.akka" %% "akka-actor"  % Version.Akka,
  "com.typesafe.akka" %% "akka-stream" % Version.Akka
)