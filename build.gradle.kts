plugins {
    kotlin("jvm") version "1.3.70"
    java
    id("com.github.johnrengelman.shadow") version "5.2.0"
    `maven-publish`
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
    val kotlinTestVersion by extra("3.4.2")
    implementation(kotlin("stdlib-jdk8"))
    implementation("com.github.thake.logminer", "logminer-sql-parser", "1.0.0-SNAPSHOT")
    implementation("net.openhft", "chronicle-queue", "5.17.40")
    implementation("io.github.microutils", "kotlin-logging", "1.7.7")
    compileOnly("org.apache.kafka", "connect-api", kafkaVersion)
    testRuntimeOnly("com.oracle.ojdbc", "ojdbc8", "19.3.0.0")
    testImplementation("org.apache.kafka", "connect-api", kafkaVersion)
    testImplementation(platform("org.testcontainers:testcontainers-bom:1.12.5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.1.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.1.0")
    testImplementation("org.testcontainers", "junit-jupiter")
    testImplementation("org.testcontainers:oracle-xe")
    testImplementation("ch.qos.logback", "logback-classic", "1.2.3")
    testImplementation("ch.qos.logback", "logback-core", "1.2.3")
    testImplementation("io.kotlintest:kotlintest-runner-junit5:$kotlinTestVersion")
    testImplementation("io.kotlintest:kotlintest-assertions:$kotlinTestVersion")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifact(tasks["kotlinSourcesJar"])
        }
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
        useJUnitPlatform()
        /*
         {
            includeEngines("junit-jupiter")
        }
         */
    }
    shadowJar {
        archiveBaseName.set("logminer-kafka-connect")
    }
    idea {
        module {
            isDownloadSources = true
            isDownloadJavadoc = false
        }
    }
    kotlinSourcesJar {
        archiveClassifier.set("sources")
        from(sourceSets.main.get().allSource)
    }
}
tasks.register<Copy>("prepare-confluent-hub") {

}
tasks.register<Zip>("confluent-hub") {
    archiveFileName.set("thake-logminer-kafka-connect-${project.version}.zip")
    dependsOn(configurations.runtimeClasspath)
    into("lib") {
        from(configurations.runtime.get().allArtifacts.files)
        from({
            configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }
        })
    }
    into("doc") {
        from("LICENSE", "Readme.md")
    }
    into("etc") {
        from("logminer-kafka-connect.properties")
    }
    into("") {
        from("manifest.json") {
            this.expand(
                "version" to project.version
            )
        }
    }
}
