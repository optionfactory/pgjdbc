# Builds and installs the driver fork in the local Maven repository (~/.m2), for teams without
# an internal Maven server. The version is pgjdbc.version in gradle.properties, without -SNAPSHOT.

GRADLE := ./gradlew
GRADLE_FLAGS := -Prelease -Psigning.pgp.enabled=OFF

.DEFAULT_GOAL := build
.PHONY: build install clean

build:
	$(GRADLE) $(GRADLE_FLAGS) :postgresql:assemble

# Gradle builds the jars first if they are missing or out of date.
install:
	$(GRADLE) $(GRADLE_FLAGS) :postgresql:publishToMavenLocal

clean:
	$(GRADLE) clean
