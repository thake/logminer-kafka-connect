import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    kotlin("jvm") version "1.3.61"
    java
    id("com.github.johnrengelman.shadow") version "4.0.4"
    idea
}

group = "com.github.thake.logminer"
version = "1.0-SNAPSHOT"



repositories {
    mavenCentral()
    mavenLocal()
    jcenter()
    maven {
        url = uri("https://maven.ceon.pl/artifactory/repo")
    }
}

dependencies {
    val kafkaVersion by extra("2.4.0")
    implementation(kotlin("stdlib-jdk8"))
    implementation("com.github.thake.logminer", "logminer-sql-parser", "1.0.0-SNAPSHOT")
    implementation("net.openhft", "chronicle-queue", "5.17.40")
    compile("io.github.microutils", "kotlin-logging", "1.7.7")
    compileOnly("org.apache.kafka", "connect-api", kafkaVersion)
    testRuntimeOnly("com.oracle.ojdbc", "ojdbc8", "19.3.0.0")
    testImplementation("org.apache.kafka", "connect-api", kafkaVersion)
    testImplementation(platform("org.testcontainers:testcontainers-bom:1.12.5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.1.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.1.0")
    testImplementation("org.testcontainers", "junit-jupiter")
    testCompile("org.testcontainers:oracle-xe")
    testImplementation("ch.qos.logback", "logback-classic", "1.2.3")
    testImplementation("ch.qos.logback", "logback-core", "1.2.3")
}

tasks {
    named<ShadowJar>("shadowJar") {
        mergeServiceFiles()
    }
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
    build {
        dependsOn(shadowJar)
    }
    test {
        useJUnitPlatform {
            includeEngines("junit-jupiter")
        }
    }
    idea {
        module {
            isDownloadSources = true
            isDownloadJavadoc = false
        }
    }
}
