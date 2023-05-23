plugins {
    scala
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.scala-lang:scala3-library_3:3.3.0")
    testImplementation("org.scalatestplus:scalacheck-1-17_3:3.2.15.0")
    testImplementation("org.scalatest:scalatest-flatspec_3:3.2.15")

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

// credit: https://stackoverflow.com/a/27096395
tasks.register("spec", JavaExec::class.java) {
    dependsOn("testClasses")
    mainClass.set("org.scalatest.tools.Runner")
    classpath = sourceSets["test"].runtimeClasspath
    args = listOf(
            "-R", "build/classes/scala/test",
            // show Duration and Full stacktrace
            "-oDF"
    )
}

tasks.test {
    dependsOn("spec")
}
