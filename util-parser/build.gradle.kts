plugins {
    id("io.github.kory33.guardedqueries.java-library-conventions")
}

dependencies {
    implementation(project(mapOf(
            "path" to ":guarded-saturation-wrapper",
            "configuration" to "gsatJar"
    )))
    implementation(project(":lib"))
    implementation("org.javafp:parsecj:0.6")
}
