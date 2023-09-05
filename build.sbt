import sbt.*

import scala.sys.process.*

ThisBuild / scalaVersion := "3.3.0"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "io.github.kory33"

ThisBuild / javacOptions ++= Seq("-encoding", "UTF-8")
ThisBuild / scalacOptions ++= Seq(
  "-Ykind-projector:underscores",
  "-deprecation",
  "-feature",
  "-unchecked"
)

ThisBuild / resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

// for Scalafix
ThisBuild / semanticdbEnabled := true
ThisBuild / scalacOptions += "-Wunused:all"

val findMavenCommand = taskKey[String]("Determine the available Maven command")
val installPdqJar = taskKey[Unit]("Download and install pdq-common to local Maven repository")
val installKaon2 = taskKey[Unit]("Install kaon2 jar to local Maven repository")
val mavenPackage = taskKey[File]("Package submodule using Maven")

lazy val root = (project in file("."))
  .aggregate(guardedSaturationWrapper, core, formulaParsers, coreIntegrationTests, app)

lazy val guardedSaturationWrapper = project
  .in(file("guarded-saturation-wrapper"))
  .settings(
    findMavenCommand := {
      def testMvnCommand(mvnExecutableName: String): Boolean = {
        try {
          Process(mvnExecutableName, Seq("-version")).!!
          true
        } catch {
          case e: Exception => false
        }
      }

      List("mvn", "mvn.cmd").find(testMvnCommand) match {
        case Some(mvnExecutableName) => mvnExecutableName
        case None                    => throw new Exception("Could not find Maven executable")
      }
    },
    installPdqJar := {
      val pdqCommonVersion = "2.0.0"
      val localTemporaryJarPath =
        s"guarded-saturation-wrapper/external_maven_dependencies/pdq-common-$pdqCommonVersion.jar"
      val pdqCommonJarUrl =
        s"https://github.com/ProofDrivenQuerying/pdq/releases/download/v$pdqCommonVersion/pdq-common-$pdqCommonVersion.jar"

      // download the jar if it's not already present
      val outputFile = file(localTemporaryJarPath)
      if (!outputFile.exists()) {
        outputFile.getParentFile.mkdirs()
        (new java.net.URL(pdqCommonJarUrl) #> outputFile).!!
      }

      // install the jar to the local Maven repository unless we previously saw the JAR file
      val foundMavenCommand = findMavenCommand.value
      FileFunction.cached(
        streams.value.cacheDirectory / "install-pdq-jar",
        inStyle = FilesInfo.lastModified
      ) { _ =>
        Process(
          Seq(
            foundMavenCommand,
            "org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file",
            // relative to the Guarded-saturation directory
            s"-Dfile=../../$localTemporaryJarPath"
          ),
          file("guarded-saturation-wrapper/Guarded-saturation")
        ).!

        Set.empty
      }(Set(file(localTemporaryJarPath)))
    },
    installKaon2 := {
      val foundMavenCommand = findMavenCommand.value

      FileFunction.cached(
        streams.value.cacheDirectory / "install-kaon2",
        inStyle = FilesInfo.lastModified
      ) { _ =>
        Process(
          Seq(
            foundMavenCommand,
            "install:install-file",
            // relative to the Guarded-saturation directory
            "-Dfile=src/main/resources/kaon2.jar",
            "-DgroupId=org.semanticweb.kaon2",
            "-DartifactId=kaon2",
            "-Dversion=2008-06-29",
            "-Dpackaging=jar",
            "-DgeneratePom=true"
          ),
          file("guarded-saturation-wrapper/Guarded-saturation")
        ).!

        Set.empty
      }(Set(file("guarded-saturation-wrapper/Guarded-saturation/src/main/resources/kaon2.jar")))
    },
    mavenPackage := {
      // task dependencies
      installPdqJar.value
      installKaon2.value

      val foundMavenCommand = findMavenCommand.value
      val cachedMvnPackage =
        FileFunction.cached(
          streams.value.cacheDirectory / "maven-package",
          inStyle = FilesInfo.lastModified,
          outStyle = FilesInfo.exists
        ) { _ =>
          Process(
            // some tests in GSat just fail right now, so ignore tests
            Seq(foundMavenCommand, "package", "-DskipTests"),
            file("guarded-saturation-wrapper/Guarded-saturation")
          ).!

          Set(file(
            "guarded-saturation-wrapper/Guarded-saturation/target/guarded-saturation-1.0.0-jar-with-dependencies.jar"
          ))
        }

      val inputFiles =
        (file("guarded-saturation-wrapper/Guarded-saturation") ** "*") ---
          (file("guarded-saturation-wrapper/Guarded-saturation/target") ** "*")
      cachedMvnPackage(inputFiles.get.toSet).head
    }
  )

lazy val core = project
  .in(file("core"))
  .settings(
    Compile / unmanagedJars += (guardedSaturationWrapper / mavenPackage).value,
    libraryDependencies ++= Seq(
      "org.scalatestplus" %% "scalacheck-1-17" % "3.2.16.0" % Test,
      "org.scalatest" %% "scalatest-flatspec" % "3.2.16" % Test
    )
  )

lazy val formulaParsers = project
  .in(file("formula-parsers"))
  .dependsOn(core)
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0"
    )
  )

lazy val coreIntegrationTests = project
  .in(file("core-integration-tests"))
  .dependsOn(core, formulaParsers)
  .settings(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest-flatspec" % "3.2.16" % Test
    ),
    Test / logBuffered := true,
    Test / baseDirectory := (ThisBuild / baseDirectory).value
  )

lazy val app = project
  .in(file("app"))
  .dependsOn(core, formulaParsers)
  .settings(
    Compile / mainClass := Some("io.github.kory33.guardedqueries.app.App"),
    assembly / assemblyOutputPath := baseDirectory.value / "target" / "guarded-saturation-app-0.1.0.jar"
  )
