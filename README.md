# Fenzo

## Overview

A generic task scheduler Java library for Apache Mesos frameworks, with plug-ins support for scheduling optimizations.

Apache Mesos frameworks eventually match and assign resources to pending tasks. Fenzo presents a plug-ins based, generic  
Java library for scheduling resources to tasks with a variety of scheduling objectives possible, such as bin packing, 
balancing across resource abstractions (e.g., AWS availability zones or data center racks), resource affinity and 
task locality. 

Fenzo Features:

- generic task scheduler for any Apache Mesos frameworks
- well suited for long running service style tasks as well as for batch or interactive use cases
- built-in support for autoscaling execution hosts cluster based on demand for resources
- plugins for constraints to effect resource selection and task placement optimizations 
    - Multiple individual plugins can be composed together to achieve higher level complex objectives. For example, 
        achieve bin packing as well as task affinity. 
    - constraints can be soft (best effort) or hard (must satisfy)
- assign CPU, memory, network bandwidth, disk, and port resources from resource offers
- support for grouping a heterogeneous mix of execution hosts based on attributes
    - each group can be autoscaled independently. E.g., keep min idle of 5 for big memory hosts and min idle of 8 for small memory hosts
- support resource allocation limits per job group
    - can define limits on use of resource amounts per job group
    - scale up of  cluster stops when limits reached, even if tasks are pending
- resource allocation failures available for monitoring/debugging why tasks can't be launched
- ability to trade off scheduling optimizations with speed of assignments

Beyond a first fit assignment of resources, frameworks can achieve advanced task placement optimizations using built in 
plugins or develop their own custom plugins. Frameworks can balance scheduling speed with optimal task assignment quality 
based on their needs. Built in plugins for fitness calculators and constraints include

- Bin packing fitness calculator: CPU, memory, or combined CPU and memory bin packing. Achieves packing tasks to as few 
    slaves as possible
- Slave attribute value constraint: select slave for a task only if they contain specific attribute value
- Unique slave attribute constraint: ensure co-tasks are placed on slaves with unique values for a given slave attribute 
    or host name. For example, one co-task per AWS EC2 availability zone, or one co-task per unique slave. 
- Balanced slave attribute constraint: ensure co-tasks are placed on slaves with equal number of slaves for each unique 
    value of the attribute. For example, balance all co-tasks across AWS EC2 availability zones.
- Exclusive slave constraint: ensure the slave is used solely for the task being assigned, even if additional resources 
    are available on the slave.
- Constraints can be specified as hard (must satisfy) or soft (satisfy as much as possible) constraints.

## Packages

- fenzo-core
    The core scheduler library for Apache Mesos frameworks. 
- fenzo-triggers
    Utility library for setting up triggers based on cron style specification. 

## Binaries

Binaries and dependency information for Maven, Ivy, Gradle and others can be found at [http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Ccom.netflix.fenzo.fenzo-core).


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
