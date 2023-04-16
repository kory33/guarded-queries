plugins {
    id("io.github.kory33.guardedqueries.java-application-conventions")
}

dependencies {
    implementation(project(":lib"))
    implementation(project(mapOf(
            "path" to ":guarded-saturation-wrapper",
            "configuration" to "gsatJar"
    )))
}

application {
    // Define the main class for the application.
    mainClass.set("io.github.kory33.guardedqueries.app.App")
}
