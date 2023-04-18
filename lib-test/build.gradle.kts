plugins {
    scala
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.scala-lang:scala3-library_3:3.2.2")
    testImplementation("org.scalacheck:scalacheck_3:1.17.0")

    testImplementation(project(":lib"))
    testImplementation(project(mapOf(
            "path" to ":guarded-saturation-wrapper",
            "configuration" to "gsatJar"
    )))
}

tasks.withType<ScalaCompile>().configureEach {
    sourceCompatibility = "1.17"
    targetCompatibility = "1.17"
}
