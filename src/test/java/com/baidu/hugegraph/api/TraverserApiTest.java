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

import static com.baidu.hugegraph.structure.constant.Traverser.DEFAULT_PAGE_LIMIT;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.api.traverser.structure.CrosspointsRequest;
import com.baidu.hugegraph.api.traverser.structure.CustomizedCrosspoints;
import com.baidu.hugegraph.exception.ServerException;
import com.baidu.hugegraph.structure.constant.Direction;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Edges;
import com.baidu.hugegraph.structure.graph.Path;
import com.baidu.hugegraph.structure.graph.Shard;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.structure.graph.Vertices;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Utils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TraverserApiTest extends BaseApiTest {

    @BeforeClass
    public static void prepareSchemaAndGraph() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initIndexLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void testPaths() {
        Object markoId = getVertexId("person", "name", "marko");
        Object joshId = getVertexId("person", "name", "josh");
        Object lopId = getVertexId("software", "name", "lop");
        Object rippleId = getVertexId("software", "name", "ripple");

        List<Path> paths = pathsAPI.get(markoId, rippleId, Direction.BOTH,
                                        null, 3, -1L, -1L, 10);
        Assert.assertEquals(2, paths.size());
        List<Object> path1 = ImmutableList.of(markoId, joshId, rippleId);
        List<Object> path2 = ImmutableList.of(markoId, lopId, joshId, rippleId);
        List<List<Object>> expectedPaths = ImmutableList.of(path1, path2);
        Assert.assertTrue(expectedPaths.contains(paths.get(0).objects()));
        Assert.assertTrue(expectedPaths.contains(paths.get(1).objects()));
    }

    @Test
    public void testPathsWithLimit() {
        Object markoId = getVertexId("person", "name", "marko");
        Object joshId = getVertexId("person", "name", "josh");
        Object rippleId = getVertexId("software", "name", "ripple");

        List<Path> paths = pathsAPI.get(markoId, rippleId, Direction.BOTH,
                                        null, 3, -1L, -1L, 1);
        Assert.assertEquals(1, paths.size());
        List<Object> path1 = ImmutableList.of(markoId, joshId, rippleId);
        Assert.assertEquals(path1, paths.get(0).objects());
    }

    @Test
    public void testPathsWithCapacity() {
        Object markoId = getVertexId("person", "name", "marko");
        Object rippleId = getVertexId("software", "name", "ripple");

        Assert.assertThrows(ServerException.class, () -> {
            pathsAPI.get(markoId, rippleId, Direction.BOTH,
                         null, 3, -1L, 2L, 1);
        });
    }

    @Test
    public void testCrosspoints() {
        Object markoId = getVertexId("person", "name", "marko");
        Object joshId = getVertexId("person", "name", "josh");
        Object lopId = getVertexId("software", "name", "lop");
        Object peterId = getVertexId("person", "name", "peter");

        List<Path> paths = crosspointsAPI.get(markoId, peterId, Direction.OUT,
                                              null, 3, -1L, -1L, 10);
        Assert.assertEquals(2, paths.size());
        Path crosspoint1 = new Path(lopId,
                                    ImmutableList.of(markoId, lopId, peterId));
        Path crosspoint2 = new Path(lopId, ImmutableList.of(markoId, joshId,
                                                            lopId, peterId));

        List<Path> crosspoints = ImmutableList.of(crosspoint1, crosspoint2);
        Assert.assertTrue(crosspoints.contains(paths.get(0)));
        Assert.assertTrue(crosspoints.contains(paths.get(1)));
    }

    @Test
    public void testCrosspointsWithCapacity() {
        Object markoId = getVertexId("person", "name", "marko");
        Object peterId = getVertexId("person", "name", "peter");

        Assert.assertThrows(ServerException.class, () -> {
            crosspointsAPI.get(markoId, peterId, Direction.OUT,
                               null, 3, -1L, 2L, 10);
        });
    }

    @Test
    public void testKoutNearest() {
        Object markoId = getVertexId("person", "name", "marko");

        long softwareId = vertexLabelAPI.get("software").id();

        List<Object> vertices = koutAPI.get(markoId, Direction.OUT,
                                            null, 2, true, -1L, -1L, -1L);
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(softwareId + ":ripple"));
    }

    @Test
    public void testKoutAll() {
        Object markoId = getVertexId("person", "name", "marko");

        long softwareId = vertexLabelAPI.get("software").id();

        List<Object> vertices = koutAPI.get(markoId, Direction.OUT, null,
                                            2, false, -1L, -1L, -1L);
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(softwareId + ":lop"));
        Assert.assertTrue(vertices.contains(softwareId + ":ripple"));
    }

    @Test
    public void testKoutBothNearest() {
        Object markoId = getVertexId("person", "name", "marko");

        long personId = vertexLabelAPI.get("person").id();
        long softwareId = vertexLabelAPI.get("software").id();

        List<Object> vertices = koutAPI.get(markoId, Direction.BOTH,
                                            null, 2, true, -1L, -1L, -1L);
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(personId + ":peter"));
        Assert.assertTrue(vertices.contains(softwareId + ":ripple"));
    }

    @Test
    public void testKoutBothAll() {
        Object markoId = getVertexId("person", "name", "marko");

        long personId = vertexLabelAPI.get("person").id();
        long softwareId = vertexLabelAPI.get("software").id();

        List<Object> vertices = koutAPI.get(markoId, Direction.BOTH, null,
                                            2, false, -1L, -1L, -1L);
        Assert.assertEquals(5, vertices.size());
        Assert.assertTrue(vertices.contains(personId + ":marko"));
        Assert.assertTrue(vertices.contains(personId + ":josh"));
        Assert.assertTrue(vertices.contains(personId + ":peter"));
        Assert.assertTrue(vertices.contains(softwareId + ":lop"));
        Assert.assertTrue(vertices.contains(softwareId + ":ripple"));
    }

    @Test
    public void testKoutBothAllWithCapacity() {
        Object markoId = getVertexId("person", "name", "marko");

        Assert.assertThrows(ServerException.class, () -> {
            koutAPI.get(markoId, Direction.BOTH, null,
                        2, false, -1L, -1L, 1L);
        });
    }

    @Test
    public void testKneighbor() {
        Object markoId = getVertexId("person", "name", "marko");

        long personId = vertexLabelAPI.get("person").id();
        long softwareId = vertexLabelAPI.get("software").id();

        List<Object> vertices = kneighborAPI.get(markoId, Direction.OUT,
                                                 null, 2, -1L, -1L);
        Assert.assertEquals(5, vertices.size());
        Assert.assertTrue(vertices.contains(softwareId + ":lop"));
        Assert.assertTrue(vertices.contains(softwareId + ":ripple"));
        Assert.assertTrue(vertices.contains(personId + ":vadas"));
        Assert.assertTrue(vertices.contains(personId + ":josh"));
        Assert.assertTrue(vertices.contains(personId + ":marko"));
    }

    @Test
    public void testCustomizedCrosspoints() {
        Object lopId = getVertexId("software", "name", "lop");
        Object joshId = getVertexId("person", "name", "josh");
        Object rippleId = getVertexId("software", "name", "ripple");

        CrosspointsRequest.Builder builder = new CrosspointsRequest.Builder();
        builder.sources().ids(lopId, rippleId);
        builder.pathPatterns().steps().direction(Direction.IN)
                                      .labels("created").degree(-1);
        builder.withPath(true).withVertex(true).capacity(-1).limit(-1);

        CustomizedCrosspoints customizedCrosspoints =
                              customizedCrosspointsAPI.post(builder.build());
        List<Object> crosspoints = customizedCrosspoints.crosspoints();
        Assert.assertEquals(1, crosspoints.size());
        Assert.assertEquals(joshId, crosspoints.get(0));

        List<Path> paths = customizedCrosspoints.paths();
        Assert.assertEquals(2, paths.size());

        List<Object> path1 = ImmutableList.of(rippleId, joshId);
        List<Object> path2 = ImmutableList.of(lopId, joshId);
        List<List<Object>> expectedPaths = ImmutableList.of(path1, path2);
        Assert.assertTrue(expectedPaths.contains(paths.get(0).objects()));
        Assert.assertTrue(expectedPaths.contains(paths.get(1).objects()));

        Set<?> vertices = customizedCrosspoints.vertices().stream()
                                               .map(Vertex::id)
                                               .collect(Collectors.toSet());
        List<Object> expectedVids = ImmutableList.of(rippleId, joshId, lopId);
        Assert.assertTrue(expectedVids.containsAll(vertices));
    }

    @Test
    public void testVertices() {
        Object markoId = getVertexId("person", "name", "marko");
        Object vadasId = getVertexId("person", "name", "vadas");
        Object joshId = getVertexId("person", "name", "josh");
        Object peterId = getVertexId("person", "name", "peter");
        Object lopId = getVertexId("software", "name", "lop");
        Object rippleId = getVertexId("software", "name", "ripple");

        List<Object> ids = ImmutableList.of(markoId, vadasId, joshId,
                                            peterId, lopId, rippleId);
        List<Vertex> vertices = verticesAPI.list(ids);

        Assert.assertEquals(6, vertices.size());

        Assert.assertEquals(markoId, vertices.get(0).id());
        Assert.assertEquals(vadasId, vertices.get(1).id());
        Assert.assertEquals(joshId, vertices.get(2).id());
        Assert.assertEquals(peterId, vertices.get(3).id());
        Assert.assertEquals(lopId, vertices.get(4).id());
        Assert.assertEquals(rippleId, vertices.get(5).id());

        Map<String, Object> props = ImmutableMap.of("name", "josh",
                                                    "city", "Beijing",
                                                    "age", 32);
        Assert.assertEquals(props, vertices.get(2).properties());
    }

    @Test
    public void testEdges() {
        String date2012Id = getEdgeId("knows", "date", "2012-01-10");
        String date2013Id = getEdgeId("knows", "date", "2013-01-10");
        String date2014Id = getEdgeId("created", "date", "2014-01-10");
        String date2015Id = getEdgeId("created", "date", "2015-01-10");
        String date2016Id = getEdgeId("created", "date", "2016-01-10");
        String date2017Id = getEdgeId("created", "date", "2017-01-10");

        List<String> ids = ImmutableList.of(date2012Id, date2013Id, date2014Id,
                                            date2015Id, date2016Id, date2017Id);

        List<Edge> edges = edgesAPI.list(ids);

        Assert.assertEquals(6, edges.size());

        Assert.assertEquals(date2012Id, edges.get(0).id());
        Assert.assertEquals(date2013Id, edges.get(1).id());
        Assert.assertEquals(date2014Id, edges.get(2).id());
        Assert.assertEquals(date2015Id, edges.get(3).id());
        Assert.assertEquals(date2016Id, edges.get(4).id());
        Assert.assertEquals(date2017Id, edges.get(5).id());

        Map<String, Object> props = ImmutableMap.of(
                                    "date", Utils.date("2014-01-10"),
                                    "city", "Shanghai");
        Assert.assertEquals(props, edges.get(2).properties());
    }

    @Test
    public void testScanVertex() {
        List<Shard> shards = verticesAPI.shards(1 * 1024 * 1024);
        List<Vertex> vertices = new LinkedList<>();
        for (Shard shard : shards) {
            Vertices results = verticesAPI.scan(shard, null, 0L);
            vertices.addAll(ImmutableList.copyOf(results.results()));
            Assert.assertNull(results.page());
        }
        Assert.assertEquals(6, vertices.size());
    }

    @Test
    public void testScanVertexInPaging() {
        List<Shard> shards = verticesAPI.shards(1 * 1024 * 1024);
        List<Vertex> vertices = new LinkedList<>();
        for (Shard shard : shards) {
            String page = "";
            while (page != null) {
                Vertices results = verticesAPI.scan(shard, page, DEFAULT_PAGE_LIMIT);
                vertices.addAll(ImmutableList.copyOf(results.results()));
                page = results.page();
            }
        }
        Assert.assertEquals(6, vertices.size());
    }

    @Test
    public void testScanVertexInPagingWithNegativeLimit() {
        List<Shard> shards = verticesAPI.shards(1 * 1024 * 1024);
        for (Shard shard : shards) {
            String page = "";
            Assert.assertThrows(ServerException.class, () -> {
                verticesAPI.scan(shard, page, -1);
            });
        }
    }

    @Test
    public void testScanVertexWithSplitSizeLt1MB() {
        Assert.assertThrows(ServerException.class, () -> {
            verticesAPI.shards(1 * 1024 * 1024 - 1);
        });
    }

    @Test
    public void testScanEdge() {
        List<Shard> shards = edgesAPI.shards(1 * 1024 * 1024);
        List<Edge> edges = new LinkedList<>();
        for (Shard shard : shards) {
            Edges results = edgesAPI.scan(shard, null, 0L);
            Assert.assertNull(results.page());
            edges.addAll(ImmutableList.copyOf(results.results()));
        }
        Assert.assertEquals(6, edges.size());
    }

    @Test
    public void testScanEdgeInPaging() {
        List<Shard> shards = edgesAPI.shards(1 * 1024 * 1024);
        List<Edge> edges = new LinkedList<>();
        for (Shard shard : shards) {
            String page = "";
            while (page != null) {
                Edges results = edgesAPI.scan(shard, page, DEFAULT_PAGE_LIMIT);
                edges.addAll(ImmutableList.copyOf(results.results()));
                page = results.page();
            }
        }
        Assert.assertEquals(6, edges.size());
    }

    @Test
    public void testScanEdgeInPagingWithNegativeLimit() {
        List<Shard> shards = edgesAPI.shards(1 * 1024 * 1024);
        for (Shard shard : shards) {
            String page = "";
            Assert.assertThrows(ServerException.class, () -> {
                edgesAPI.scan(shard, page, -1);
            });
        }
    }

    @Test
    public void testScanEdgeWithSplitSizeLt1MB() {
        Assert.assertThrows(ServerException.class, () -> {
            edgesAPI.shards(1 * 1024 * 1024 - 1);
        });
    }
}
