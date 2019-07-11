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

package com.baidu.hugegraph.api;

import static com.baidu.hugegraph.api.graph.structure.UpdateStrategy.INTERSECTION;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.api.graph.structure.BatchVertexRequest;
import com.baidu.hugegraph.api.graph.structure.UpdateStrategy;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Utils;
import com.google.common.collect.ImmutableMap;

public class VertexApiTest extends BaseApiTest {

    @BeforeClass
    public static void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
    }

    @Override
    @After
    public void teardown() {
        vertexAPI.list(-1).results().forEach(v -> vertexAPI.delete(v.id()));
    }

    @Test
    public void testCreate() {
        Vertex vertex = new Vertex("person");
        vertex.property("name", "James");
        vertex.property("city", "Beijing");
        vertex.property("age", 19);

        vertex = vertexAPI.create(vertex);

        Assert.assertEquals("person", vertex.label());
        Map<String, Object> props = ImmutableMap.of("name", "James",
                                                    "city", "Beijing",
                                                    "age", 19);
        Assert.assertEquals(props, vertex.properties());
    }

    @Test
    public void testCreateWithUndefinedLabel() {
        Vertex vertex = new Vertex("undefined");
        vertex.property("name", "James");
        vertex.property("city", "Beijing");
        vertex.property("age", 19);

        Utils.assertResponseError(400, () -> {
            vertexAPI.create(vertex);
        });
    }

    @Test
    public void testCreateWithUndefinedProperty() {
        Vertex vertex = new Vertex("person");
        vertex.property("name", "James");
        vertex.property("not-exist-key", "not-exist-value");

        Utils.assertResponseError(400, () -> {
            vertexAPI.create(vertex);
        });
    }

    @Test
    public void testCreateWithoutPrimaryKey() {
        Vertex vertex = new Vertex("person");
        vertex.property("city", "Beijing");
        vertex.property("age", 19);

        Utils.assertResponseError(400, () -> {
            vertexAPI.create(vertex);
        });
    }

    @Test
    public void testCreateWithCustomizeStringId() {
        Vertex person = new Vertex("person");
        person.property(T.id, "123456");
        person.property("name", "James");
        person.property("city", "Beijing");
        person.property("age", 19);

        Utils.assertResponseError(400, () -> {
            vertexAPI.create(person);
        });

        Vertex book = new Vertex("book");
        book.id("ISBN-123456");
        book.property("name", "spark graphx");

        Vertex vertex = vertexAPI.create(book);
        Assert.assertEquals("book", vertex.label());
        Assert.assertEquals("ISBN-123456", vertex.id());
        Map<String, Object> props = ImmutableMap.of("name", "spark graphx");
        Assert.assertEquals(props, vertex.properties());
    }

    @Test
    public void testCreateWithCustomizeNumberId() {
        Vertex person = new Vertex("person");
        person.property(T.id, 123456);
        person.property("name", "James");
        person.property("city", "Beijing");
        person.property("age", 19);

        Utils.assertResponseError(400, () -> {
            vertexAPI.create(person);
        });

        Vertex log = new Vertex("log");
        log.id(123456);
        log.property("date", "20180101");

        Vertex vertex = vertexAPI.create(log);
        Assert.assertEquals("log", vertex.label());
        Assert.assertEquals(123456, vertex.id());
        Map<String, Object> props = ImmutableMap.of("date", "20180101");
        Assert.assertEquals(props, vertex.properties());
    }

    @Test
    public void testCreateWithNullableKeysAbsent() {
        Vertex vertex = new Vertex("person");
        // Absent prop city
        vertex.property("name", "James");
        vertex.property("age", 19);

        vertex = vertexAPI.create(vertex);

        Assert.assertEquals("person", vertex.label());
        Map<String, Object> props = ImmutableMap.of("name", "James",
                                                    "age", 19);
        Assert.assertEquals(props, vertex.properties());
    }

    @Test
    public void testCreateWithNonNullKeysAbsent() {
        Vertex vertex = new Vertex("person");
        // Absent prop 'age'
        vertex.property("name", "James");
        vertex.property("city", "Beijing");

        Utils.assertResponseError(400, () -> {
            vertexAPI.create(vertex);
        });
    }

    @Test
    public void testCreateExistVertex() {
        Vertex vertex = new Vertex("person");
        vertex.property("name", "James");
        vertex.property("city", "Beijing");
        vertex.property("age", 19);
        vertexAPI.create(vertex);

        vertex = new Vertex("person");
        vertex.property("name", "James");
        vertex.property("city", "Shanghai");
        vertex.property("age", 20);
        vertex = vertexAPI.create(vertex);

        Assert.assertEquals("person", vertex.label());
        Map<String, Object> props = ImmutableMap.of("name", "James",
                                                    "city", "Shanghai",
                                                    "age", 20);
        Assert.assertEquals(props, vertex.properties());
    }

    @Test
    public void testBatchCreate() {
        List<Vertex> vertices = super.create100PersonBatch();
        vertexAPI.create(vertices);

        List<Object> ids = vertexAPI.create(vertices);
        Assert.assertEquals(100, ids.size());
        for (int i = 0; i < 100; i++) {
            Vertex person = vertexAPI.get(ids.get(i));
            Assert.assertEquals("person", person.label());
            Map<String, Object> props = ImmutableMap.of("name", "Person-" + i,
                                                        "city", "Beijing",
                                                        "age", 30);
            Assert.assertEquals(props, person.properties());
        }
    }

    @Test
    public void testBatchUpdateStrategySum() {
        BatchVertexRequest req = batchVertexRequest("price", 1, -1,
                                                    UpdateStrategy.SUM);

        this.vertexAPI.update(req).forEach(vertex -> {
            Object price = vertex.properties().get("price");
            Assert.assertTrue(price instanceof Number);
            Assert.assertEquals(0, price);
        });
    }

    @Test
    public void testBatchUpdateStrategyBigger() {
        BatchVertexRequest req = batchVertexRequest("price", 1, -1,
                                                    UpdateStrategy.BIGGER);

        this.vertexAPI.update(req).forEach(vertex -> {
            Object price = vertex.properties().get("price");
            Assert.assertTrue(price instanceof Number);
            Assert.assertTrue((int) price > 0);
        });
    }

    @Test
    public void testBatchUpdateStrategySmaller() {
        // TODO: Add date comparison after fixing the date serialization bug
        BatchVertexRequest req = batchVertexRequest("price", -1, 1,
                                                    UpdateStrategy.SMALLER);

        this.vertexAPI.update(req).forEach(vertex -> {
            Object price = vertex.properties().get("price");
            Assert.assertTrue(price instanceof Number);
            Assert.assertTrue((int) price < 0);
        });
    }

    @Test
    public void testBatchUpdateStrategyUnion() {
        BatchVertexRequest req = batchVertexRequest("set", "old", "new",
                                                    UpdateStrategy.UNION);

        // TODO: List from server is unordered, consider better way to validate
        this.vertexAPI.update(req).forEach(vertex -> {
            Object set = vertex.properties().get("set");
            Assert.assertTrue(set instanceof Collection);
            Assert.assertEquals(2, ((Collection<?>) set).size());
        });
    }

    @Test
    public void testBatchUpdateStrategyIntersection() {
        BatchVertexRequest req = batchVertexRequest("set", "old", "new",
                                                    INTERSECTION);

        this.vertexAPI.update(req).forEach(vertex -> {
            Object set = vertex.properties().get("set");
            Assert.assertTrue(set instanceof Collection);
            Assert.assertTrue(((Collection<?>) set).isEmpty());
        });
    }

    @Test
    public void testBatchUpdateStrategyAppend() {
        BatchVertexRequest req = batchVertexRequest("list", "old", "old",
                                                    UpdateStrategy.APPEND);

        this.vertexAPI.update(req).forEach(vertex -> {
            Object list = vertex.properties().get("list");
            Assert.assertTrue(list instanceof List);
            Assert.assertEquals(2, ((List<?>) list).size());
        });
    }

    @Test
    public void testBatchUpdateStrategyEliminate() {
        BatchVertexRequest req = batchVertexRequest("list", "old", "old",
                                                    UpdateStrategy.ELIMINATE);

        this.vertexAPI.update(req).forEach(vertex -> {
            Object list = vertex.properties().get("list");
            Assert.assertTrue(list instanceof List);
            Assert.assertTrue(((List<?>) list).isEmpty());
        });
    }

    @Test
    public void testBatchCreateContainsInvalidVertex() {
        List<Vertex> vertices = super.create100PersonBatch();
        vertices.get(0).property("invalid-key", "invalid-value");
        vertices.get(10).property("not-exist-key", "not-exist-value");

        Utils.assertResponseError(400, () -> {
            vertexAPI.create(vertices);
        });
    }

    @Test
    public void testBatchCreateWithMoreThanBatchSize() {
        List<Vertex> vertices = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            Vertex vertex = new Vertex("person");
            vertex.property("name", "Person" + "-" + i);
            vertex.property("city", "Beijing");
            vertex.property("age", 20);
            vertices.add(vertex);
        }
        Utils.assertResponseError(400, () -> {
            vertexAPI.create(vertices);
        });
    }

    @Test
    public void testGet() {
        Vertex vertex1 = new Vertex("person");
        vertex1.property("name", "James");
        vertex1.property("city", "Beijing");
        vertex1.property("age", 19);

        vertex1 = vertexAPI.create(vertex1);

        Vertex vertex2 = vertexAPI.get(vertex1.id());
        Assert.assertEquals(vertex1.label(), vertex2.label());
        Assert.assertEquals(vertex1.properties(), vertex2.properties());
    }

    @Test
    public void testGetWithCustomizeStringId() {
        Vertex vertex1 = new Vertex("book");
        vertex1.id("ISBN-123456");
        vertex1.property("name", "spark graphx");

        vertex1 = vertexAPI.create(vertex1);

        Vertex vertex2 = vertexAPI.get("ISBN-123456");
        Assert.assertEquals(vertex1.label(), vertex2.label());
        Assert.assertEquals(vertex1.properties(), vertex2.properties());
    }

    @Test
    public void testGetWithCustomizeNumberId() {
        Vertex vertex1 = new Vertex("log");
        vertex1.id(123456);
        vertex1.property("date", "20180101");

        vertex1 = vertexAPI.create(vertex1);

        Vertex vertex2 = vertexAPI.get(123456);
        Assert.assertEquals(vertex1.label(), vertex2.label());
        Assert.assertEquals(vertex1.properties(), vertex2.properties());
    }

    @Test
    public void testGetNotExist() {
        Utils.assertResponseError(404, () -> {
            vertexAPI.get("not-exist-vertex-id");
        });
    }

    @Test
    public void testList() {
        List<Vertex> vertices = super.create100PersonBatch();
        vertexAPI.create(vertices);

        vertices = vertexAPI.list(-1).results();
        Assert.assertEquals(100, vertices.size());
    }

    @Test
    public void testListWithLimit() {
        List<Vertex> vertices = super.create100PersonBatch();
        vertexAPI.create(vertices);

        vertices = vertexAPI.list(10).results();
        Assert.assertEquals(10, vertices.size());
    }

    @Test
    public void testDelete() {
        Vertex vertex = new Vertex("person");
        vertex.property("name", "James");
        vertex.property("city", "Beijing");
        vertex.property("age", 19);

        vertex = vertexAPI.create(vertex);

        Object id = vertex.id();
        vertexAPI.delete(id);

        Utils.assertResponseError(404, () -> {
            vertexAPI.get(id);
        });
    }

    @Test
    public void testDeleteNotExist() {
        Utils.assertResponseError(400, () -> {
            vertexAPI.delete("not-exist-v");
        });
    }


    private BatchVertexRequest batchVertexRequest(String key, Object oldData,
                                                  Object newData,
                                                  UpdateStrategy strategy) {
        Map<String, UpdateStrategy> strategies = ImmutableMap.of(key, strategy);
        // Init old & new vertices
        this.graph().addVertices(this.createNVertexBatch("testV", oldData, 5));
        List<Vertex> vertices = this.createNVertexBatch("testV", newData, 5);

        BatchVertexRequest req;
        req = new BatchVertexRequest.Builder().vertices(vertices)
                                              .updateStrategies(strategies)
                                              .createIfNotExist(true)
                                              .build();
        return req;
    }

    @SuppressWarnings("unused")
    private static void assertContains(List<Vertex> vertices, Vertex vertex) {
        Assert.assertTrue(Utils.contains(vertices, vertex));
    }
}
