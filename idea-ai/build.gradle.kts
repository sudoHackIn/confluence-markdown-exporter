plugins {
    kotlin("jvm") version "2.0.21"
    id("org.jetbrains.intellij.platform") version "2.2.1"
}

group = "com.example.ideaai"
version = "0.1.0"

repositories {
    mavenCentral()
    intellijPlatform {
        defaultRepositories()
    }
}

dependencies {
    intellijPlatform {
        intellijIdeaUltimate("2025.1")
        bundledPlugin("com.intellij.java")
        testFramework(org.jetbrains.intellij.platform.gradle.TestFrameworkType.Platform)
    }
}

kotlin {
    jvmToolchain(21)
}

intellijPlatform {
    pluginConfiguration {
        id = "com.example.ideaai"
        name = "Idea AI Helper"
        version = project.version.toString()

        ideaVersion {
            sinceBuild = "251"
            untilBuild = "252.*"
        }

        vendor {
            name = "Vladislav"
            email = "vladislav@example.com"
        }
    }
}

tasks {
    patchPluginXml {
        changeNotes = "Initial project skeleton"
    }

    test {
        useJUnitPlatform()
    }
}
