/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.functional;

import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableMap;

public class HugeClientHttpsTest extends BaseFuncTest {

    private static final String BASE_URL = "https://127.0.0.1:8443";
    private static final String GRAPH = "hugegraph";
    private static final String USERNAME = "";
    private static final String PASSWORD = "";
    private static final int TIMEOUT = 10;
    private static final int MAX_CONNS_PER_ROUTE = 10;
    private static final int MAX_CONNS = 10;
    private static final int IDLE_TIME = 30;
    private static final String TRUST_STORE_FILE = "src/test/resources/hugegraph-client.jks";
    private static final String TRUST_STORE_PASSWORD = "hugegraph";

    private static HugeClient client;

    @After
    public void teardown() throws Exception {
        Assert.assertNotNull("Not opened client", client);
        client.close();
    }

    @Test
    public void testHttpsClientBuilderWithConnection() {
        client = HugeClient.builder(BASE_URL, GRAPH)
                           .configSSL(TRUST_STORE_FILE, TRUST_STORE_PASSWORD)
                           .build();
        Assert.assertTrue(client.graphs().listGraph().contains("hugegraph"));
        this.addVertexAndCheckPropertyValue();
    }

    @Test
    public void testHttpsClientWithConnectionPoolNoUserParam() {
        client = HugeClient.builder(BASE_URL, GRAPH)
                           .configTimeout(TIMEOUT)
                           .configPool(MAX_CONNS, MAX_CONNS_PER_ROUTE)
                           .configSSL(TRUST_STORE_FILE, TRUST_STORE_PASSWORD)
                           .build();
        Assert.assertTrue(client.graphs().listGraph().contains("hugegraph"));
        this.addVertexAndCheckPropertyValue();
    }

    @Test
    public void testHttpsClientWithConnectionPoolNoTimeOutParam() {
        client = HugeClient.builder(BASE_URL, GRAPH)
                           .configUser(USERNAME, PASSWORD)
                           .configPool(MAX_CONNS, MAX_CONNS_PER_ROUTE)
                           .configSSL(TRUST_STORE_FILE, TRUST_STORE_PASSWORD)
                           .build();
        Assert.assertTrue(client.graphs().listGraph().contains("hugegraph"));
        this.addVertexAndCheckPropertyValue();
    }

    @Test
    public void testHttpsClientNewBuilderWithConnectionNoPoolParam() {
        client = HugeClient.builder(BASE_URL, GRAPH)
                           .configUser(USERNAME, PASSWORD)
                           .configTimeout(TIMEOUT)
                           .configSSL(TRUST_STORE_FILE, TRUST_STORE_PASSWORD)
                           .build();
        Assert.assertTrue(client.graphs().listGraph().contains("hugegraph"));
        this.addVertexAndCheckPropertyValue();
    }

    @Test
    public void testHttpsClientNewBuilderWithConnectionPool() {
        client = HugeClient.builder(BASE_URL, GRAPH)
                           .configUser(USERNAME, PASSWORD)
                           .configTimeout(TIMEOUT)
                           .configPool(MAX_CONNS, MAX_CONNS_PER_ROUTE)
                           .configSSL(TRUST_STORE_FILE, TRUST_STORE_PASSWORD)
                           .configIdleTime(IDLE_TIME)
                           .build();
        Assert.assertTrue(client.graphs().listGraph().contains("hugegraph"));
        this.addVertexAndCheckPropertyValue();
    }

    @Test
    public void testHttpsClientNewBuilderZeroPoolParam() {
        client = HugeClient.builder(BASE_URL, GRAPH)
                           .configUser(USERNAME, PASSWORD)
                           .configTimeout(TIMEOUT)
                           .configPool(0, 0)
                           .configSSL(TRUST_STORE_FILE, TRUST_STORE_PASSWORD)
                           .build();
        Assert.assertTrue(client.graphs().listGraph().contains("hugegraph"));
        this.addVertexAndCheckPropertyValue();
    }

    @Test
    public void testHttpsClientBuilderWithConnectionPoolNoParam() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            HugeClient.builder(BASE_URL, GRAPH)
                      .configUrl(null)
                      .configGraph(null)
                      .configSSL("", "")
                      .build();
        }, e -> {
            Assert.assertContains("The url parameter can't be null",
                                  e.getMessage());
        });
    }

    @Test
    public void testHttpsClientBuilderWithConnectionPoolNoGraphParam() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            HugeClient.builder(BASE_URL, GRAPH)
                      .configGraph(null)
                      .configSSL("", "")
                      .build();
        }, e -> {
            Assert.assertContains("The graph parameter can't be null",
                                  e.getMessage());
        });
    }

    @Test
    public void testHttpsClientBuilderWithConnectionPoolZeroIdleTimeParam() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            HugeClient.builder(BASE_URL, GRAPH)
                      .configIdleTime(0)
                      .build();
        }, e -> {
            Assert.assertContains("The idleTime parameter must be > 0, but got",
                                  e.getMessage());
        });
    }

    private void addVertexAndCheckPropertyValue() {
        SchemaManager schema = client.schema();
        schema.propertyKey("name").asText().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("city").asText().ifNotExist().create();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .ifNotExist()
              .create();
        GraphManager graph = client.graph();
        Vertex marko = graph.addVertex(T.label, "person", "name", "marko",
                                       "age", 29, "city", "Beijing");
        Map<String, Object> props = ImmutableMap.of("name", "marko",
                                                    "age", 29, "city", "Beijing");
        Assert.assertEquals(props, marko.properties());
    }
}
