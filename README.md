Chinese-Whispers
================

A generic gossip based state replication and failure detection service.  The network transport is UDP unicast.

This project is licensed under the [Apache license, version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

Read the [project wiki](https://github.com/Hellblazer/Chinese-Whispers/wiki) for more information on design, configuration and usage.

This project requires Maven version 3.x to build.

To build this project, cd to the root directory and do:

    mvn clean install

See the [project wiki](https://github.com/Hellblazer/Chinese-Whispers/wiki) for design and usage.

### Maven configuration

For releases, include the hellblazer release repository:

    <repository>
        <id>hellblazer-release</id>
        <url>https://repository-hal900000.forge.cloudbees.com/release/</url>
    </repository>
    
add as dependency:

    <dependency>
        <groupId>com.hellblazer</groupId>
        <artifactId>chinese-whispers</artifactId>
        <version>1.0.0</version>
    </dependency>

For snapshots, include the hellblazer snapshot repository:

    <repository>
        <id>hellblazer-snapshots</id>
        <url>https://repository-hal900000.forge.cloudbees.com/snapshot/</url>
    </repository>
    
add as dependency:

    <dependency>
        <groupId>com.hellblazer</groupId>
        <artifactId>chinese-whispers</artifactId>
        <version>1.0.1-SNAPSHOT</version>
    </dependency>
