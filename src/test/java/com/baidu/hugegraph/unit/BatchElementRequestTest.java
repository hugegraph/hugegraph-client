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

package com.baidu.hugegraph.unit;

import static com.baidu.hugegraph.api.graph.structure.UpdateStrategy.INTERSECTION;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.baidu.hugegraph.BaseClientTest;
import com.baidu.hugegraph.api.graph.structure.BatchEdgeRequest;
import com.baidu.hugegraph.api.graph.structure.BatchVertexRequest;
import com.baidu.hugegraph.api.graph.structure.UpdateStrategy;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class BatchElementRequestTest extends BaseClientTest {

    @Test
    public void testVertexEmptyUpdateStrategy() {
        List<Vertex> vertices = this.createNVertexBatch("object", "new", 5);
        Map<String, UpdateStrategy> strategies = Collections.emptyMap();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new BatchVertexRequest.Builder().vertices(vertices)
                                            .updatingStrategies(strategies)
                                            .createIfNotExist(true)
                                            .build();
        });
    }

    @Test
    public void testVertexNotSupportedUpdateParameter() {
        List<Vertex> vertices = this.createNVertexBatch("object", "new", 5);
        Map<String, UpdateStrategy> strategies = ImmutableMap.of("set",
                                                                 INTERSECTION);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new BatchVertexRequest.Builder().vertices(vertices)
                                            .updatingStrategies(strategies)
                                            .createIfNotExist(false)
                                            .build();
        });
    }

    @Test
    public void testEdgeEmptyUpdateStrategy() {
        Edge edge = new Edge("updates");
        edge.id("object:1>updates>>object:2");
        edge.sourceId("object:1");
        edge.sourceLabel("object");
        edge.targetId("object:2");
        edge.targetLabel("object");
        edge.property("price", 1);

        List<Edge> edges = ImmutableList.of(edge);
        Map<String, UpdateStrategy> strategies = Collections.emptyMap();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new BatchEdgeRequest.Builder().edges(edges)
                                          .updatingStrategies(strategies)
                                          .checkVertex(false)
                                          .createIfNotExist(true)
                                          .build();
        });
    }

    @Test
    public void testEdgeNotSupportedUpdateParameter() {
        Edge edge = new Edge("updates");
        edge.id("object:1>updates>>object:2");
        edge.sourceId("object:1");
        edge.sourceLabel("object");
        edge.targetId("object:2");
        edge.targetLabel("object");
        edge.property("price", 1);

        List<Edge> edges = ImmutableList.of(edge);
        Map<String, UpdateStrategy> strategies = ImmutableMap.of("set",
                                                                 INTERSECTION);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new BatchEdgeRequest.Builder().edges(edges)
                                          .updatingStrategies(strategies)
                                          .checkVertex(false)
                                          .createIfNotExist(false)
                                          .build();
        });
    }
}
