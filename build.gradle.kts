import org.jetbrains.kotlin.konan.util.visibleName

plugins {
    java
    `java-library`
    idea
    kotlin("jvm") version "1.5.31"
    kotlin("plugin.serialization") version "1.5.31"
}

group = "com.codality.data.tools"
version = "1.1-SNAPSHOT"
description = "Data tools for codality ETL"

//logging.captureStandardOutput(LogLevel.INFO)

java {
    sourceCompatibility = JavaVersion.VERSION_1_9
    targetCompatibility = JavaVersion.VERSION_1_9
}

tasks.compileKotlin {
    kotlinOptions {
        jvmTarget = "9"
    }
}

tasks.compileTestKotlin {
    kotlinOptions {
        jvmTarget = "9"
    }
}

repositories {
    mavenLocal()
    mavenCentral()
}

sourceSets.main {
    java.srcDirs("src/main/java", "src/main/kotlin")
}

sourceSets.test {
    java.srcDirs("src/test/java", "src/test/kotlin")
}


val grpcVersion = "1.42.1"
val protobufDepVersion = "3.19.1"

dependencies {
    compileOnly("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protobufDepVersion")
    implementation("org.slf4j:slf4j-api:1.7.32")
    implementation("org.slf4j:slf4j-log4j12:1.7.32")
    implementation("log4j:log4j:1.2.17")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.11.2")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.11.2")
    implementation("io.lettuce:lettuce-core:5.2.1.RELEASE")
    implementation("org.mongodb:mongo-java-driver:3.12.0")
    implementation("com.google.code.gson:gson:2.8.9")
    implementation("com.codality.data.tools:proto:1.1-SNAPSHOT")
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.5.31")
    implementation(platform("org.jetbrains.kotlin:kotlin-bom:1.5.31"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib:1.5.31")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.3.1")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-protobuf:1.3.1")
    testImplementation("io.grpc:grpc-protobuf:$grpcVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:5.8.1")
    testImplementation(kotlin("test"))
    testImplementation("de.bwaldvogel:mongo-java-server:1.24.0")
    testImplementation("ai.grakn:redis-mock:0.1.3")

}

tasks {
    test {
        useJUnitPlatform()
        testLogging.showExceptions = true
        testLogging.showCauses = true
        testLogging.showStandardStreams = true
    }
}