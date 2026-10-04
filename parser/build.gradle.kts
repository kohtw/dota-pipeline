plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.skadistats:clarity:5.0.0")
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
