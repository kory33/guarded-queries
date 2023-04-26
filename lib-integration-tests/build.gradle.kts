import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    id("io.github.kory33.guardedqueries.java-library-conventions")
}

dependencies {
    testImplementation(project(":lib"))
    testImplementation(project(mapOf(
            "path" to ":guarded-saturation-wrapper",
            "configuration" to "gsatJar"
    )))
    testImplementation("org.javafp:parsecj:0.6")
}

tasks.test {
    maxHeapSize = "10240m"
    workingDir = rootProject.projectDir

    testLogging {
        events = setOf(TestLogEvent.STANDARD_OUT)
    }
}
