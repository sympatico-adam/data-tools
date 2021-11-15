import com.google.protobuf.gradle.*
import org.gradle.kotlin.dsl.provider.gradleKotlinDslOf

plugins {
    java
    `java-library`
    idea
    id("maven-publish")
    id("com.google.protobuf") version "0.8.17"
    kotlin("jvm")

}

repositories {
    maven("https://plugins.gradle.org/m2/")
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

group = "com.codality.data.tools"
version = "1.1-SNAPSHOT"
description = "Codality protobuf library"

val protobufDepVersion = "3.19.1"
val grpcVersion = "1.42.1"

dependencies {
    compileOnly("com.google.protobuf:protobuf-java:$protobufDepVersion")
    compileOnly("io.grpc:grpc-stub:$grpcVersion")
    compileOnly("io.grpc:grpc-protobuf:$grpcVersion")
    if (JavaVersion.current().isJava9Compatible) {
        // Workaround for @javax.annotation.Generated
        // see: https://github.com/grpc/grpc-java/issues/3633
        compileOnly("javax.annotation:javax.annotation-api:1.3.1")
    }
}

publishing {
    publications {
        create<MavenPublication>("proto") {
            from(components["java"])
        }
    }

    repositories {
        maven {
            name = "proto"
            url = uri(layout.buildDirectory.dir("../repo"))
        }
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufDepVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    task("builtins") {
        java { }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach { task ->
            task.plugins {
                id("grpc")
            }
            task.generateDescriptorSet = true
        }
    }
}