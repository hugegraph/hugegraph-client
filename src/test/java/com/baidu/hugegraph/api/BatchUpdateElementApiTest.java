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

import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.api.graph.structure.BatchEdgeRequest;
import com.baidu.hugegraph.api.graph.structure.BatchVertexRequest;
import com.baidu.hugegraph.api.graph.structure.UpdateStrategy;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.google.common.collect.ImmutableMap;

public class BatchUpdateElementApiTest extends BaseApiTest {

    final int number = 5;

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
        edgeAPI.list(-1).results().forEach(e -> edgeAPI.delete(e.id()));
    }

    /* Vertex Test */
    @Test
    public void testVertexBatchUpdateStrategySum() {
        BatchVertexRequest req = batchVertexRequest("price", 1, -1,
                                                    UpdateStrategy.SUM);
        List<Vertex> vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "price", 0);

        req = batchVertexRequest("price", 2, 3, UpdateStrategy.SUM);
        vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "price", 5);
    }

    @Test
    public void testVertexBatchUpdateStrategyBigger() {
        // TODO: Add date comparison after fixing the date serialization bug
        BatchVertexRequest req = batchVertexRequest("price", -3, 1,
                                                    UpdateStrategy.BIGGER);

        List<Vertex> vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "price", 1);

        req = batchVertexRequest("price", 7, 3, UpdateStrategy.BIGGER);
        vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "price", 7);
    }

    @Test
    public void testVertexBatchUpdateStrategySmaller() {
        BatchVertexRequest req = batchVertexRequest("price", -3, 1,
                                                    UpdateStrategy.SMALLER);

        List<Vertex> vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "price", -3);

        req = batchVertexRequest("price", 7, 3, UpdateStrategy.SMALLER);
        vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "price", 3);
    }

    @Test
    public void testVertexBatchUpdateStrategyUnion() {
        BatchVertexRequest req = batchVertexRequest("set", "old", "new",
                                                    UpdateStrategy.UNION);

        List<Vertex> vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "set", "new", "old");

        req = batchVertexRequest("set", "old", "old", UpdateStrategy.UNION);
        vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "set", "old");
    }

    @Test
    public void testVertexBatchUpdateStrategyIntersection() {
        BatchVertexRequest req = batchVertexRequest("set", "old", "new",
                                                    INTERSECTION);

        List<Vertex> vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "set");

        req = batchVertexRequest("set", "old", "old", INTERSECTION);
        vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "set", "old");
    }

    @Test
    public void testVertexBatchUpdateStrategyAppend() {
        BatchVertexRequest req = batchVertexRequest("list", "old", "old",
                                                    UpdateStrategy.APPEND);

        List<Vertex> vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "list", "old", "old");

        req = batchVertexRequest("list", "old", "new", UpdateStrategy.APPEND);
        vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "list", "old", "new");
    }

    @Test
    public void testVertexBatchUpdateStrategyEliminate() {
        BatchVertexRequest req = batchVertexRequest("list", "old", "old",
                                                    UpdateStrategy.ELIMINATE);

        List<Vertex> vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "list");

        req = batchVertexRequest("list", "old", "x", UpdateStrategy.ELIMINATE);
        vertices = this.vertexAPI.update(req);
        this.assertBatchResponse(vertices, "list", "old");
    }

    /* Edge Test */
    @Test
    public void testEdgeBatchUpdateStrategySum() {
        BatchEdgeRequest req = batchEdgeRequest("price", -1, 1,
                                                UpdateStrategy.SUM);
        List<Edge> edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "price", 0);

        req = batchEdgeRequest("price", 2, 3, UpdateStrategy.SUM);
        edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "price", 5);
    }

    @Test
    public void testEdgeBatchUpdateStrategyBigger() {
        // TODO: Add date comparison after fixing the date serialization bug
        BatchEdgeRequest req = batchEdgeRequest("price", -3, 1,
                                                UpdateStrategy.BIGGER);
        List<Edge> edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "price", 1);

        req = batchEdgeRequest("price", 7, 3, UpdateStrategy.BIGGER);
        edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "price", 7);
    }

    @Test
    public void testEdgeBatchUpdateStrategySmaller() {
        BatchEdgeRequest req = batchEdgeRequest("price", -3, 1,
                                                UpdateStrategy.SMALLER);
        List<Edge> edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "price", -3);

        req = batchEdgeRequest("price", 7, 3, UpdateStrategy.SMALLER);
        edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "price", 3);
    }

    @Test
    public void testEdgeBatchUpdateStrategyUnion() {
        BatchEdgeRequest req = batchEdgeRequest("set", "old", "new",
                                                UpdateStrategy.UNION);
        List<Edge> edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "set", "new", "old");

        req = batchEdgeRequest("set", "old", "old", UpdateStrategy.UNION);
        edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "set", "old");
    }

    @Test
    public void testEdgeBatchUpdateStrategyIntersection() {
        BatchEdgeRequest req = batchEdgeRequest("set", "old", "new",
                                                INTERSECTION);
        List<Edge> edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "set");

        req = batchEdgeRequest("set", "old", "old", INTERSECTION);
        edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "set", "old");
    }

    @Test
    public void testEdgeBatchUpdateStrategyAppend() {
        BatchEdgeRequest req = batchEdgeRequest("list", "old", "old",
                                                UpdateStrategy.APPEND);
        List<Edge> edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "list", "old", "old");

        req = batchEdgeRequest("list", "old", "new", UpdateStrategy.APPEND);
        edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "list", "old", "new");
    }

    @Test
    public void testEdgeBatchUpdateStrategyEliminate() {
        BatchEdgeRequest req = batchEdgeRequest("list", "old", "old",
                                                UpdateStrategy.ELIMINATE);
        List<Edge> edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "list");

        req = batchEdgeRequest("list", "old", "new", UpdateStrategy.ELIMINATE);
        edges = this.edgeAPI.update(req);
        this.assertBatchResponse(edges, "list", "old");
    }

    private BatchVertexRequest batchVertexRequest(String key, Object oldData,
                                                  Object newData,
                                                  UpdateStrategy strategy) {
        // Init old & new vertices
        this.graph().addVertices(this.createNVertexBatch("object", oldData,
                                                         number));
        List<Vertex> vertices = this.createNVertexBatch("object", newData,
                                                        number);

        Map<String, UpdateStrategy> strategies = ImmutableMap.of(key, strategy);
        BatchVertexRequest req;
        req = new BatchVertexRequest.Builder().vertices(vertices)
                                              .updatingStrategies(strategies)
                                              .createIfNotExist(true)
                                              .build();
        return req;
    }

    private BatchEdgeRequest batchEdgeRequest(String key, Object oldData,
                                              Object newData,
                                              UpdateStrategy strategy) {
        // Init old vertices & edges
        this.graph().addVertices(this.createNVertexBatch("object", oldData,
                                                         number * 2));
        this.graph().addEdges(this.createNEdgesBatch("object", "updates",
                                                     oldData, number));

        List<Edge> edges = this.createNEdgesBatch("object", "updates",
                                                  newData, number);

        Map<String, UpdateStrategy> strategies = ImmutableMap.of(key, strategy);
        BatchEdgeRequest req;
        req = new BatchEdgeRequest.Builder().edges(edges)
                                            .updatingStrategies(strategies)
                                            .checkVertex(false)
                                            .createIfNotExist(true)
                                            .build();
        return req;
    }
}
