plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.skadistats:clarity:5.0.0")
    implementation("software.amazon.awssdk:s3:2.29.0")  // pre-2.30: avoids default CRC checksums S3-compatibles may reject
    runtimeOnly("org.slf4j:slf4j-simple:2.0.17")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

application {
    mainClass.set("ReplayInfo")
}
