import com.google.protobuf.gradle.*
import org.gradle.api.internal.HasConvention
import org.gradle.kotlin.dsl.provider.gradleKotlinDslOf

plugins {
    kotlin("jvm") version "1.5.10"
    id("com.google.protobuf")
    idea
}

group = "com.codality.data.tools"
version = "1.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
}