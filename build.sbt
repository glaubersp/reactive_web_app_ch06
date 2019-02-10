import sbt.util

name := """reactive_web_app_ch06"""
organization := "com.glauber"
version := "1.0-SNAPSHOT"

scalaVersion := "2.12.8"
logLevel := util.Level.Debug

lazy val root = (project in file(".")).enablePlugins(PlayScala)

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += guice
libraryDependencies += ws
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "4.0.1" % Test
libraryDependencies += "org.reactivemongo" %% "play2-reactivemongo" % "0.16.0-play26"

//routesGenerator := InjectedRoutesGenerator

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "com.example.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "com.example.binders._"
