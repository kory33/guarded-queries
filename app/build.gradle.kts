plugins {
    id("io.github.kory33.guardedqueries.java-application-conventions")
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

dependencies {
    implementation(project(mapOf(
            "path" to ":guarded-saturation-wrapper",
            "configuration" to "gsatJar"
    )))
    implementation(project(":lib"))
    implementation(project(":util-parser"))
    implementation("org.javafp:parsecj:0.6")
}

application {
    mainClass.set("io.github.kory33.guardedqueries.app.App")
}

tasks {
    shadowJar {
        archiveBaseName.set("guarded-saturation")
        archiveVersion.set("0.1.0")
    }
}
