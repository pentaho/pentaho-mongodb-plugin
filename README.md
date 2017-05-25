Pentaho Big Data Plugin
=======================

The Pentaho Big Data Plugin Project provides support for an ever-expanding Big Data community within the Pentaho ecosystem. It is a plugin for the Pentaho Kettle engine which can be used within Pentaho Data Integration (Kettle), Pentaho Reporting, and the Pentaho BI Platform.

Building
--------
The Pentaho Big Data Plugin is built with Apache maven and uses maven for dependency management. All you'll need to get started is to start maven 3.0.1 or newer version to build the project. 

* git clone git://github.com/pentaho/big-data-plugin.git
* cd big-data-plugin
* mvn clean install


This will produce a plugin archive in target/pentaho-big-data-plugin-${project.revision}.tar.gz (and .zip). This archive can then be extracted into your Pentaho Data Integration plugin directory.

#### Pre-requisites for building the project:
* Maven, version 3+
* Java JDK 1.8
* This [settings.xml](https://github.com/pentaho/maven-parent-poms/blob/master/maven-support-files/settings.xml) in your <user-home>/.m2 directory

#### Building it

__Build for nightly/release__

All required profiles are activated by the presence of a property named "release".

```
$ mvn clean install -Drelease
```

This will build, unit test, and package the whole project (all of the sub-modules). The artifact will be generated in: ```target```

__Build for CI/dev__

The `release` builds will compile the source for production (meaning potential obfuscation and/or uglification). To build without that happening, just eliminate the `release` property.

```
$ mvn clean install
```

#### Running the tests

__Unit tests__

This will run all tests in the project (and sub-modules).
```
$ mvn test
```

If you want to remote debug a single java unit test (default port is 5005):
```
$ cd core
$ mvn test -Dtest=<<YourTest>> -Dmaven.surefire.debug
```

Further Reading
---------------
Additional documentation is available on the Community wiki: [Big Data Plugin for Java Developers](http://wiki.pentaho.com/display/BAD/Getting+Started+for+Java+Developers)

License
-------
Licensed under the Apache License, Version 2.0. See LICENSE.txt for more information.