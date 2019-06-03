scalaVersion := "2.12.8"

//val kafkaVersion = "2.1.1"
val kafkaVersion = "2.2.0"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion
libraryDependencies += "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"
