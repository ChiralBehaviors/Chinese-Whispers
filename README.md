Chinese-Whispers
================

A generic gossip based state replication and failure detection service.  The network transport is UDP.

This project is licensed under the [http://www.apache.org/licenses/LICENSE-2.0](Apache license, version 2.0).

This project requires Maven version 3.x to build.  This project also depends on the [https://github.com/Hellblazer/Utils](Hellblazer Utils project).  Until that project is posted in a maven repository, you'll have to acquire it and build it before building this project.

To build this project, cd to the root directory and do:

    mvn clean install

See the [https://github.com/Hellblazer/Chinese-Whispers/wiki](project wiki) for design and usage.