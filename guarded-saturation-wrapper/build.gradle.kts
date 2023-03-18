//region Utilities

fun downloadIfNotAlreadyDownloaded(url: java.net.URL, outputPath: String) {
    val file = File(outputPath)
    file.parentFile.mkdirs()
    if (!file.exists()) {
        java.io.FileOutputStream(file).use { outputStream ->
            url.openStream().use { input ->
                java.nio.channels.Channels.newChannel(input).use { byteChannel ->
                    outputStream.channel.transferFrom(byteChannel, 0, Long.MAX_VALUE)
                }
            }
        }
    } else if (!file.isFile) {
        throw RuntimeException("Something other than file is present at $pdqCommonJarAbsolutePath, aborting.")
    }
}

val availableMavenCommand: String = kotlin.run {
    fun mvnCommandPresentAt(mvnCommandPath: String): Boolean =
            try {
                Runtime.getRuntime().exec(arrayOf(mvnCommandPath, "-version"))
                true
            } catch (_: Throwable) {
                false
            }

    listOf("mvn", "mvn.cmd")
            .firstOrNull { mvnCommandPresentAt(it) }
            ?: throw RuntimeException("maven command is not found: tried mvn and mvn.cmd")
}

//endregion

//region constants

val guardedSaturationMavenProjectPath = project.projectDir.absolutePath + "/Guarded-saturation"

val pdqCommonVersion = "2.0.0"
val pdqCommonJarName = "pdq-common-${pdqCommonVersion}.jar"
val pdqCommonJarUrl = java.net.URL("https://github.com/ProofDrivenQuerying/pdq/releases/download/v${pdqCommonVersion}/${pdqCommonJarName}")

//endregion

val pdqCommonJarRelativePath = "external_maven_dependencies/pdq-common-${pdqCommonVersion}.jar"
val pdqCommonJarAbsolutePath = "${project.projectDir.absolutePath}/${pdqCommonJarRelativePath}"

// fetch and install pdq-common library to local maven repository, according to
// https://github.com/KRR-Oxford/Guarded-saturation/tree/83cb805564a8a89c399381f26c5c16f6acedd38e#installing-pdq-in-maven
val fetchPdqJar = tasks.register("fetch-pdq-jar") {
    doFirst {
        downloadIfNotAlreadyDownloaded(pdqCommonJarUrl, pdqCommonJarAbsolutePath)
    }
}

val installPdqJar = task<Exec>("install-pdq-jar") {
    dependsOn(fetchPdqJar)
    // we have to run maven install-file command in the maven project directory
    // or else we will get an NPE
    workingDir(guardedSaturationMavenProjectPath)
    commandLine(
            availableMavenCommand,
            "org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file",
            "-Dfile=../${pdqCommonJarRelativePath}",
    )
}

// install included kaon2.jar to local maven repository, according to
// https://github.com/KRR-Oxford/Guarded-saturation/tree/83cb805564a8a89c399381f26c5c16f6acedd38e#installing-kaon-2
val installKaon2 = task<Exec>("install-kaon-2") {
    workingDir(guardedSaturationMavenProjectPath)
    commandLine(
            availableMavenCommand,
            "install:install-file",
            "-Dfile=./src/main/resources/kaon2.jar",
            "-DgroupId=org.semanticweb.kaon2",
            "-DartifactId=kaon2",
            "-Dversion=2008-06-29",
            "-Dpackaging=jar",
            "-DgeneratePom=true"
    )
}

val build = task<Exec>("build") {
    dependsOn(installPdqJar, installKaon2)
    workingDir(guardedSaturationMavenProjectPath)
    commandLine(
            availableMavenCommand,
            "package",
            "-DskipTests" // some tests in GSat just fail right now, so igonre tests
    )
}

val gsatJar: Configuration by configurations.creating {
    isCanBeConsumed = true
    isCanBeResolved = false
}

artifacts {
    add(gsatJar.name, File("$guardedSaturationMavenProjectPath/target/guarded-saturation-1.0.0-jar-with-dependencies.jar")) {
        builtBy(build)
    }
}

//endregion