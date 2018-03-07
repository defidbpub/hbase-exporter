=======
HBase-Exporter
=======

HBase-Exporter: Apache HBase table exporter written in Scala/Spark

.. image:: https://img.shields.io/badge/HBase_Exporter-v1.0.0-green.svg
        :target: https://github.com/janrock-hwx?tab=repositories
        :alt: Release Status

Features
--------

| This code provides an example of usage hadoop.hbase libs to export content of HBase table to structured CSV format file.

Functionality Highlights:
 - Connection string to Zookeeper allow to use the code out of cluster

Coming soon:
 - Currently parked
 - Next version 0.2 will have external args

Usage
-----
1) The easiest way to clone the repository: git clone ...
2) sbt clean, build, run (assembly)

Version Support
---------------
v0.1: Initial Commit

Requirements
------------
JDK8, Scala, Apache-Spark
