plugins {
    id("io.github.kory33.guardedqueries.java-library-conventions")
}

dependencies {
    testImplementation(project(":lib"))
    testImplementation(project(mapOf(
            "path" to ":guarded-saturation-wrapper",
            "configuration" to "gsatJar"
    )))
}