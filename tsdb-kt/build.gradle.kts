import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.10"
}

group = "com.ganmacs"
version = "0.1"

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.github.microutils:kotlin-logging:1.12.5")
    implementation("org.slf4j:slf4j-simple:1.7.29")

    implementation(platform("io.micrometer:micrometer-bom:1.8.2"))
    implementation("io.micrometer:micrometer-registry-prometheus")

    implementation("com.google.guava:guava:31.0.1-jre")

    testImplementation(kotlin("test"))

    testImplementation(platform("org.junit:junit-bom:5.8.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.mockk:mockk:1.12.1")

}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "11"
}