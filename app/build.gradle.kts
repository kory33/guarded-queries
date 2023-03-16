plugins {
    id("io.github.kory33.guardedqueries.java-application-conventions")
}

dependencies {
    implementation("org.apache.commons:commons-text")
    implementation(project(":lib"))
}

application {
    // Define the main class for the application.
    mainClass.set("io.github.kory33.guardedqueries.app.App")
}
