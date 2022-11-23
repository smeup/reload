# Maven Central deploy

## Steps

### Build

Build project with Maven including sources and (javadocs)[https://github.com/Kotlin/dokka].

### Sign

Sign all jars with gpg (signature plugin)[https://maven.apache.org/plugins/maven-gpg-plugin/].

### Deploy

Deploy all artifact to (Sonatype Nexus)[https://s01.oss.sonatype.org/], close staging repository and release to maven central.

## Command

To execute automatically all steps run:

```
mvn -B clean pre-site deploy -DautoDropAfterRelease=true -DskipTests -Pci-cd
```