Chinese-Whispers
================

A generic gossip based state replication and failure detection service.  The network transport is UDP unicast.

This project is licensed under the [Apache license, version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

This project requires Maven version 3.x to build.

To build this project, cd to the root directory and do:

    mvn clean install

See the [project wiki](https://github.com/Hellblazer/Chinese-Whispers/wiki) for design and usage.

### Maven configuration

include the hellblazer snapshot repository:

    <repository>
        <id>hellblazer-snapshots</id>
        <url>https://repository-hal900000.forge.cloudbees.com/snapshot/</url>
    </repository>
    
add as dependency:

    <dependency>
        <groupId>com.hellblazer</groupId>
        <artifactId>chinese-whispers</artifactId>
        <version>0.0.1-SNAPSHOT</version>
    </dependency>
