# Fenzo

## Overview

Fenzo is a scheduler Java library for Apache Mesos frameworks that supports plugins for scheduling
optimizations and facilitates cluster autoscaling.

Apache Mesos frameworks match and assign resources to pending tasks. Fenzo presents a plugin-based, Java library that 
facilitates scheduling resources to tasks by using a variety of possible scheduling objectives,
such as bin packing, balancing across resource abstractions (such as AWS availability zones or data center
racks), resource affinity, and task locality. 

Fenzo Features:

- it is a generic task scheduler that works with any Apache Mesos frameworks
- it is well suited both for long running service-style tasks and for batch or interactive use cases
- it can autoscale the execution hosts cluster based on demand for resources
- it supports plugins with which you can customize the constraints that regulate resource selection and task
  placement optimizations 
    - you can compose multiple individual plugins together to achieve higher level complex objectives; for
      example, you can in this way achieve bin packing as well as task affinity
    - the constraints you choose can be soft (best effort) or hard (must satisfy)
- you can assign CPU, memory, network bandwidth, disk, and port resources from resource offers
- you can group a heterogeneous mix of execution hosts based on their attributes
    - you can have each group autoscaled independently; for example, you may choose to keep a minimum of
      five big-memory hosts idle, but a minimum of eight small-memory hosts
- you can set resource allocation limits on a per job group basis
    - you can define limits on the use of resource amounts on a per job group basis
    - the scale up of the cluster stops when these limits are reached, even if tasks are pending
- you can monitor resource allocation failures in order to assist in debugging why some tasks can't be launched
- you have the ability to trade off scheduling optimizations with speed of assignments

Beyond a first-fit assignment of resources, Fenzo frameworks can optimize task placement by using the built-in
plugins, or they can design and deploy their own custom task placement optimization plugins. Frameworks can
balance scheduling speed with optimal task assignment quality based on their needs. The fitness calculator and
constraint plugins that are built in to Fenzo include:

- Bin packing fitness calculator
    - CPU, memory, network bandwidth, or a combination of them
    - packs tasks into as few hosts as possible
- Host attribute value constraint
    - selects a host for a task only if it has a specific attribute value
- Unique host attribute constraint
    - ensures co-tasks are placed on hosts that have unique values for a given attribute or host attrName
    - for example: one co-task per AWS EC2 availability zone, or one co-task per unique host
- Balanced host attribute constraint
    - ensure co-tasks are placed on hosts such that there are an equal number of tasks on hosts with unique value 
      of the attribute
    - for example: balance all co-tasks across AWS EC2 availability zones
- Exclusive host constraint
    - ensure the host is used solely for the task being assigned, even if additional resources are available on
      the host

You can specify whether Fenzo applies a constraint in a hard (must satisfy) or soft (satisfy as much as possible)
manner.

## Packages

- fenzo-core
    The core scheduler library for Apache Mesos frameworks. 
- fenzo-triggers
    Utility library for setting up triggers based on cron style specification. 

## Binaries

Binaries and dependency information for Maven, Ivy, Gradle and others can be found at
[http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Ccom.netflix.fenzo.fenzo-core).

## Javadocs

- [fenzo-core](http://netflix.github.io/Fenzo/fenzo-core/index.html)
- [fenzo-triggers](http://netflix.github.io/Fenzo/fenzo-triggers/index.html)

## Programmer's Guide

[The Fenzo Programmer's Guide](https://github.com/Netflix/Fenzo/wiki) is available as a wiki on
[the Fenzo GitHub site](https://github.com/Netflix/Fenzo/).

## LICENSE

Copyright 2015 Netflix, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
