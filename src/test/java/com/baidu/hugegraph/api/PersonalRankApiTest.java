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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.api.traverser.PersonalRankAPI;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableMap;

public class PersonalRankApiTest extends BaseApiTest {

    @BeforeClass
    public static void initPersonalRankGraph() {
        GraphManager graph = graph();
        SchemaManager schema = schema();

        schema.propertyKey("name").asText().ifNotExist().create();

        schema.vertexLabel("person")
              .properties("name")
              .useCustomizeStringId()
              .ifNotExist()
              .create();

        schema.vertexLabel("movie")
              .properties("name")
              .useCustomizeStringId()
              .ifNotExist()
              .create();

        schema.edgeLabel("like")
              .sourceLabel("person")
              .targetLabel("movie")
              .ifNotExist()
              .create();

        Vertex A = graph.addVertex(T.label, "person", T.id, "A", "name", "A");
        Vertex B = graph.addVertex(T.label, "person", T.id, "B", "name", "B");
        Vertex C = graph.addVertex(T.label, "person", T.id, "C", "name", "C");

        Vertex a = graph.addVertex(T.label, "movie", T.id, "a", "name", "a");
        Vertex b = graph.addVertex(T.label, "movie", T.id, "b", "name", "b");
        Vertex c = graph.addVertex(T.label, "movie", T.id, "c", "name", "c");
        Vertex d = graph.addVertex(T.label, "movie", T.id, "d", "name", "d");

        A.addEdge("like", a);
        A.addEdge("like", c);

        B.addEdge("like", a);
        B.addEdge("like", b);
        B.addEdge("like", c);
        B.addEdge("like", d);

        C.addEdge("like", c);
        C.addEdge("like", d);
    }

    @AfterClass
    public static void clearPersonalRankGraph() {
        List<Long> taskIds = new ArrayList<>();
        taskIds.add(edgeLabelAPI.delete("like"));
        taskIds.forEach(BaseApiTest::waitUntilTaskCompleted);
        taskIds.clear();
        taskIds.add(vertexLabelAPI.delete("movie"));
        taskIds.add(vertexLabelAPI.delete("person"));
        taskIds.forEach(BaseApiTest::waitUntilTaskCompleted);
    }

    @Test
    public void testPersonalRank() {
        PersonalRankAPI.Request.Builder builder;
        builder = PersonalRankAPI.Request.builder();
        builder.source("A").label("like").alpha(0.9).maxDepth(50);
        PersonalRankAPI.Request request = builder.build();

        Map<Object, Double> ranks = personalRankAPI.post(request);
        Map<Object, Double> expectedRanks = ImmutableMap.of(
                "B", 0.2065750574989044D,
                "C", 0.09839507219265439D,
                "d", 0.08959757100230095D,
                "b", 0.04589958822642998D
        );
        Assert.assertEquals(expectedRanks, ranks);
    }

    @Test
    public void testPersonalRankWithWithLabel() {
        PersonalRankAPI.Request.Builder builder;
        builder = PersonalRankAPI.Request.builder();
        builder.source("A").label("like").alpha(0.9).maxDepth(50)
               .withLabel(PersonalRankAPI.Request.WithLabel.SAME_LABEL);
        PersonalRankAPI.Request request = builder.build();

        Map<Object, Double> ranks = personalRankAPI.post(request);
        Map<Object, Double> expectedRanks = ImmutableMap.of(
                "B", 0.2065750574989044D,
                "C", 0.09839507219265439D
        );
        Assert.assertEquals(expectedRanks, ranks);

        builder = PersonalRankAPI.Request.builder();
        builder.source("A").label("like").alpha(0.9).maxDepth(50)
               .withLabel(PersonalRankAPI.Request.WithLabel.OTHER_LABEL);
        request = builder.build();

        ranks = personalRankAPI.post(request);
        expectedRanks = ImmutableMap.of(
                "d", 0.08959757100230095D,
                "b", 0.04589958822642998D
        );
        Assert.assertEquals(expectedRanks, ranks);
    }

    @Test
    public void testPersonalRankWithOtherAlpha() {
        PersonalRankAPI.Request.Builder builder;
        builder = PersonalRankAPI.Request.builder();
        builder.source("A").label("like").alpha(1).maxDepth(50);
        PersonalRankAPI.Request request = builder.build();

        Map<Object, Double> ranks = personalRankAPI.post(request);
        Map<Object, Double> expectedRanks = ImmutableMap.of(
                "B", 0.5D,
                "C", 0.24999999999999956D,
                "b", 0.0D,
                "d", 0.0D
        );
        Assert.assertEquals(expectedRanks, ranks);
    }

    @Test
    public void testPersonalRankWithDegree() {
        PersonalRankAPI.Request.Builder builder;
        builder = PersonalRankAPI.Request.builder();

        builder.source("A").label("like").alpha(0.9).degree(1).maxDepth(1);
        PersonalRankAPI.Request request = builder.build();

        // Removed root and direct neighbors of root
        Map<Object, Double> ranks = personalRankAPI.post(request);
        Assert.assertEquals(ImmutableMap.of(), ranks);

        builder.source("A").label("like").alpha(0.9).degree(1).maxDepth(2);
        request = builder.build();

        ranks = personalRankAPI.post(request);
        Assert.assertEquals(ImmutableMap.of(), ranks);

        builder.source("A").label("like").alpha(0.9).degree(2).maxDepth(1);
        request = builder.build();

        ranks = personalRankAPI.post(request);
        Assert.assertEquals(ImmutableMap.of(), ranks);

        builder.source("A").label("like").alpha(0.9).degree(2).maxDepth(2);
        request = builder.build();

        ranks = personalRankAPI.post(request);
        Assert.assertEquals(ImmutableMap.of("B", 0.405D), ranks);
    }

    @Test
    public void testPersonalRankWithLimit() {
        PersonalRankAPI.Request.Builder builder;
        builder = PersonalRankAPI.Request.builder();
        builder.source("A").label("like").alpha(0.9).limit(3).maxDepth(50);
        PersonalRankAPI.Request request = builder.build();

        Map<Object, Double> ranks = personalRankAPI.post(request);
        Map<Object, Double> expectedRanks = ImmutableMap.of(
                "B", 0.2065750574989044D,
                "C", 0.09839507219265439D,
                "d", 0.08959757100230095D
        );
        Assert.assertEquals(expectedRanks, ranks);
    }

    @Test
    public void testPersonalRankWithMaxDepth() {
        PersonalRankAPI.Request.Builder builder;
        builder = PersonalRankAPI.Request.builder();
        builder.source("A").label("like").alpha(0.9).maxDepth(20);
        PersonalRankAPI.Request request = builder.build();

        Map<Object, Double> ranks = personalRankAPI.post(request);
        Map<Object, Double> expectedRanks = ImmutableMap.of(
                "B", 0.23414889646372697D,
                "C", 0.11218194186115384D,
                "d", 0.07581065434649958D,
                "b", 0.03900612828909826D
        );
        Assert.assertEquals(expectedRanks, ranks);
    }

    @Test
    public void testPersonalRankWithUnsorted() {
        PersonalRankAPI.Request.Builder builder;
        builder = PersonalRankAPI.Request.builder();
        builder.source("A").label("like").alpha(0.9).maxDepth(50).sorted(false);
        PersonalRankAPI.Request request = builder.build();

        Map<Object, Double> ranks = personalRankAPI.post(request);
        Map<Object, Double> expectedRanks = ImmutableMap.of(
                "b", 0.04589958822642998D,
                "B", 0.2065750574989044D,
                "C", 0.09839507219265439D,
                "d", 0.08959757100230095D
        );
        Assert.assertEquals(expectedRanks, ranks);
    }

    @Test
    public void testPersonalRankWithIsolatedVertex() {
        Vertex isolate = graph().addVertex(T.label, "person", T.id, "isolate",
                                           "name", "isolate-vertex");

        PersonalRankAPI.Request.Builder builder;
        builder = PersonalRankAPI.Request.builder();
        builder.source("isolate").label("like").alpha(0.9).maxDepth(50);
        PersonalRankAPI.Request request = builder.build();

        Map<Object, Double> ranks = personalRankAPI.post(request);
        Assert.assertEquals(ImmutableMap.of(), ranks);

        graph().removeVertex(isolate.id());
    }

    @Test
    public void testPersonalRankWithInvalidParams() {
        // Invalid source
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            PersonalRankAPI.Request.Builder builder;
            builder = PersonalRankAPI.Request.builder();
            builder.source(null);
        });

        // Invalid label
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            PersonalRankAPI.Request.Builder builder;
            builder = PersonalRankAPI.Request.builder();
            builder.label(null);
        });

        // Invalid alpha
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            PersonalRankAPI.Request.Builder builder;
            builder = PersonalRankAPI.Request.builder();
            builder.alpha(0.0);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            PersonalRankAPI.Request.Builder builder;
            builder = PersonalRankAPI.Request.builder();
            builder.alpha(1.1);
        });

        // Invalid degree
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            PersonalRankAPI.Request.Builder builder;
            builder = PersonalRankAPI.Request.builder();
            builder.degree(-2);
        });

        // Invalid limit
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            PersonalRankAPI.Request.Builder builder;
            builder = PersonalRankAPI.Request.builder();
            builder.limit(-2);
        });

        // Invalid maxDepth
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            PersonalRankAPI.Request.Builder builder;
            builder = PersonalRankAPI.Request.builder();
            builder.maxDepth(0);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            PersonalRankAPI.Request.Builder builder;
            builder = PersonalRankAPI.Request.builder();
            builder.maxDepth(51);
        });
    }
}
