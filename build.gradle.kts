import org.jetbrains.kotlin.konan.util.visibleName

plugins {
    `java-library`
    idea
    kotlin("jvm") version "1.5.31"
    kotlin("plugin.serialization") version "1.5.31"
}

group = "com.codality.data.tools"
version = "1.1-SNAPSHOT"
description = "Data tools for codality ETL"

java {
    sourceCompatibility = JavaVersion.VERSION_1_9
    targetCompatibility = JavaVersion.VERSION_1_9
}

tasks.compileKotlin {
    sourceCompatibility = "1.9"
    targetCompatibility = "1.9"
}

apply {
    plugin("java")
    kotlinTestRegistry
}

repositories {
    mavenLocal()
    mavenCentral()
}

val grpcVersion = "1.42.1"

dependencies {
    compileOnly("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.ktor:ktor-client-logging-jvm:1.6.2")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.14.1")
    implementation("org.slf4j:slf4j-api:1.7.30")
    implementation("log4j:log4j:1.2.17")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.11.2")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.11.2")
    implementation("org.apache.commons:commons-lang3:3.9")
    implementation("io.lettuce:lettuce-core:5.2.1.RELEASE")
    implementation("org.mongodb:mongo-java-driver:3.12.0")
    implementation("org.codehaus.jettison:jettison:1.3.7")
    implementation("com.google.code.gson:gson:2.8.9")
    implementation("com.codality.data.tools:proto:1.1-SNAPSHOT")
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.5.31")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.3.1")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-protobuf:1.3.1")
    testImplementation("io.grpc:grpc-protobuf:$grpcVersion")
    testImplementation("com.codality.data.tools:proto:1.1-SNAPSHOT")
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.2")
    testImplementation("de.bwaldvogel:mongo-java-server:1.24.0")
    testImplementation("ai.grakn:redis-mock:0.1.3")
}

tasks.withType<JavaCompile>() {
    options.encoding = "UTF-8"
}

sourceSets {
    main {
        java {
            srcDir("src/main/kotlin")
        }
    }

    test {
        java {
            srcDir("src/test/kotlin")
        }
    }
}



tasks.test {
    useJUnitPlatform()
    maxHeapSize = "1G"
    testLogging {
        events("passed", "skipped", "failed", "standardOut", "standardError")
    }
    doFirst { kotlinTestRegistry.registerTestTask(tasks.test) }
    doLast {
        executeTests()
    }

}
