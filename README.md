# BREAKING Change:

Refer https://github.com/apache/incubator-hugegraph-toolchain, Users should use the Apache version now, visit apache repository instead, thanks~

```xml
  <!-- Note: use the latest release version in maven repo, here is just an example -->
  <dependency>
       <groupId>org.apache.hugegraph</groupId>
       <artifactId>hugegraph-client</artifactId>
       <!-- Update it to the latest release version -->
       <version>1.2.0</version>
  </dependency>

  <dependency>
       <groupId>org.apache.hugegraph</groupId>
       <artifactId>hugegraph-loader</artifactId>
       <version>1.2.0</version>
  </dependency>
```

Related client-doc https://hugegraph.apache.org/docs/quickstart/hugegraph-client/

---

# hugegraph-client (**Outdated**)

[![License](https://img.shields.io/badge/license-Apache%202-0E78BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Build Status](https://travis-ci.org/hugegraph/hugegraph-client.svg?branch=master)](https://travis-ci.org/hugegraph/hugegraph-client)
[![codecov](https://codecov.io/gh/hugegraph/hugegraph-client/branch/master/graph/badge.svg)](https://codecov.io/gh/hugegraph/hugegraph-client)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.baidu.hugegraph/hugegraph-client/badge.svg)](https://mvnrepository.com/artifact/com.baidu.hugegraph/hugegraph-client)

hugegraph-client is a Java-written client of [HugeGraph](https://github.com/apache/hugegraph), providing operations of graph, schema, gremlin, variables and traversals etc. All these operations are interpreted and translated into RESTful requests to HugeGraph Server. Besides, hugegraph-client also checks arguments, serializes and deserializes structures and encapsulates server exceptions.

## Features

- Graph Operation, CRUD of vertexes and edges, batch load of vertexes and edges
- Schema Operation, CRUD of vertex label, edge label, index label and property key
- Gremlin Traversal Statements
- RESTful Traversals, shortest path, k-out, k-neighbor, paths and crosspoints etc.
- Variables, CRUD of variables

## Licence
The same as HugeGraph, hugegraph-client is also licensed under Apache 2.0 License.
