// Replay extractor jar for Spark (see ReplayExtractor.java, spark_jobs/).
// ./gradlew extractorJars -> build/extractor/*.jar  (extractor + Clarity), passed to Spark as spark.jars
// ./gradlew s3aJars       -> build/s3a/*.jar        (Spark's s3a:// connector, for Garage)
plugins {
    java
}

repositories {
    mavenCentral()
}

// s3a connector: hadoop-aws must match the Hadoop that pyspark ships (3.4.1 for Spark 4.0.1),
// and the AWS SDK bundle must be the one that hadoop-aws release was built against
val s3a: Configuration by configurations.creating { isTransitive = false }

dependencies {
    implementation("com.skadistats:clarity:5.0.0")
    compileOnly("org.apache.spark:spark-sql_2.13:4.0.1")  // provided by Spark / PySpark at runtime
    s3a("org.apache.hadoop:hadoop-aws:3.4.1")
    s3a("software.amazon.awssdk:bundle:2.24.6")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.register<Copy>("extractorJars") {
    from(tasks.jar)
    from(configurations.runtimeClasspath)
    into(layout.buildDirectory.dir("extractor"))
}

tasks.register<Copy>("s3aJars") {
    from(s3a)
    into(layout.buildDirectory.dir("s3a"))
}

// Small offline checks use Java assertions and the already-resolved compile jars.
sourceSets.test {
    compileClasspath += configurations.compileClasspath.get()
}
tasks.register<JavaExec>("extractorTests") {
    dependsOn(tasks.testClasses)
    classpath = sourceSets["test"].runtimeClasspath + configurations.compileClasspath.get()
    mainClass.set("ReplayExtractorTest")
    enableAssertions = true
}
