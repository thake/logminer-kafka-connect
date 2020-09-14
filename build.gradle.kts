plugins {
    kotlin("jvm") version "1.3.71"
    java
    id("com.github.johnrengelman.shadow") version "5.2.0"
    signing
    idea
    distribution
    id("net.researchgate.release") version "2.8.1"
}

group = "com.github.thake.logminer"

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
    val junitVersion by extra("5.7.0")
    implementation(kotlin("stdlib-jdk8"))
    implementation("com.github.thake.logminer", "logminer-sql-parser", "0.1.0")
    implementation("net.openhft", "chronicle-queue", "5.17.40")
    implementation("io.github.microutils", "kotlin-logging", "1.7.7")
    compileOnly("org.apache.kafka", "connect-api", kafkaVersion)
    testRuntimeOnly("com.oracle.ojdbc", "ojdbc8", "19.3.0.0")
    testImplementation("org.apache.kafka", "connect-api", kafkaVersion)
    testImplementation(platform("org.testcontainers:testcontainers-bom:1.12.5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.testcontainers", "junit-jupiter")
    testImplementation("org.testcontainers:oracle-xe")
    testImplementation("ch.qos.logback", "logback-classic", "1.2.3")
    testImplementation("ch.qos.logback", "logback-core", "1.2.3")
    testImplementation("io.kotlintest:kotlintest-runner-junit5:$kotlinTestVersion")
    testImplementation("io.kotlintest:kotlintest-assertions:$kotlinTestVersion")
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
distributions {
    main {
        //The main distribution is in a format needed for confluent-hub:
        //https://docs.confluent.io/current/connect/managing/confluent-hub/component-archive.html
        baseName = "thake-${project.name}"
        contents {
            into("lib") {
                from(tasks["jar"])
                from(configurations.runtimeClasspath)
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
                        "version" to project.version,
                        "name" to project.name
                    )
                }
            }
        }
    }
}
signing {
    useGpgCmd()
    isRequired = !isSnapshot
    sign(tasks["distZip"])
}

inline val Project.isSnapshot
    get() = version.toString().endsWith("-SNAPSHOT")
