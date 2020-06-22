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

package com.baidu.hugegraph.api.traverser;

import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.exception.ServerException;
import com.baidu.hugegraph.structure.constant.Direction;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.structure.traverser.CountRequest;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CountApiTest extends TraverserApiTest {

    @BeforeClass
    public static void initGraph() {
        schema().propertyKey("time")
                .asDate()
                .ifNotExist()
                .create();

        schema().propertyKey("weight")
                .asDouble()
                .ifNotExist()
                .create();

        schema().vertexLabel("node")
                .useCustomizeStringId()
                .ifNotExist()
                .create();

        schema().edgeLabel("link")
                .sourceLabel("node").targetLabel("node")
                .properties("time")
                .multiTimes().sortKeys("time")
                .ifNotExist()
                .create();

        schema().edgeLabel("relateTo")
                .sourceLabel("node").targetLabel("node")
                .properties("weight")
                .ifNotExist()
                .create();

        Vertex va = graph().addVertex(T.label, "node", T.id, "A");
        Vertex vb = graph().addVertex(T.label, "node", T.id, "B");
        Vertex vc = graph().addVertex(T.label, "node", T.id, "C");
        Vertex vd = graph().addVertex(T.label, "node", T.id, "D");
        Vertex ve = graph().addVertex(T.label, "node", T.id, "E");
        Vertex vf = graph().addVertex(T.label, "node", T.id, "F");
        Vertex vg = graph().addVertex(T.label, "node", T.id, "G");
        Vertex vh = graph().addVertex(T.label, "node", T.id, "H");
        Vertex vi = graph().addVertex(T.label, "node", T.id, "I");
        Vertex vj = graph().addVertex(T.label, "node", T.id, "J");
        Vertex vk = graph().addVertex(T.label, "node", T.id, "K");
        Vertex vl = graph().addVertex(T.label, "node", T.id, "L");
        Vertex vm = graph().addVertex(T.label, "node", T.id, "M");
        Vertex vn = graph().addVertex(T.label, "node", T.id, "N");
        Vertex vo = graph().addVertex(T.label, "node", T.id, "O");
        Vertex vp = graph().addVertex(T.label, "node", T.id, "P");
        Vertex vq = graph().addVertex(T.label, "node", T.id, "Q");
        Vertex vr = graph().addVertex(T.label, "node", T.id, "R");
        Vertex vs = graph().addVertex(T.label, "node", T.id, "S");
        Vertex vt = graph().addVertex(T.label, "node", T.id, "T");
        Vertex vu = graph().addVertex(T.label, "node", T.id, "U");
        Vertex vv = graph().addVertex(T.label, "node", T.id, "V");
        Vertex vw = graph().addVertex(T.label, "node", T.id, "W");
        Vertex vx = graph().addVertex(T.label, "node", T.id, "X");
        Vertex vy = graph().addVertex(T.label, "node", T.id, "Y");
        Vertex vz = graph().addVertex(T.label, "node", T.id, "Z");

        /*
         *
         *             c -----> f
         *            ^
         *           / d -----> g
         *          / ^
         *         / /
         *        b ---> e -----> h
         *       ^
         *      /     j <----- m
         *     /     /
         *    /     /
         *   /     <
         * a <--- i <--- k <--- n
         *   .     ^
         *    .     \
         *     .     \
         *      .     l <------ o
         *       >
         *        p ...> q ...> v
         *          ...> r ...> w
         *          ...> s ...> x
         *          ...> t ...> y
         *          ...> u ...> z
         *
         * Description:
         * 1. ">","<","^" means arrow
         * 2. "---" means "link" edge
         * 3. "..." means "relateTo" edge
         *
         */
        va.addEdge("link", vb, "time", "2020-01-01");

        vb.addEdge("link", vc, "time", "2020-01-02");
        vb.addEdge("link", vd, "time", "2020-01-03");
        vb.addEdge("link", ve, "time", "2020-01-04");

        vc.addEdge("link", vf, "time", "2020-01-05");
        vd.addEdge("link", vg, "time", "2020-01-06");
        ve.addEdge("link", vh, "time", "2020-01-07");

        vi.addEdge("link", va, "time", "2020-01-08");

        vj.addEdge("link", vi, "time", "2020-01-09");
        vk.addEdge("link", vi, "time", "2020-01-10");
        vl.addEdge("link", vi, "time", "2020-01-11");

        vm.addEdge("link", vj, "time", "2020-01-12");
        vn.addEdge("link", vk, "time", "2020-01-13");
        vo.addEdge("link", vl, "time", "2020-01-14");

        va.addEdge("relateTo", vp, "weight", 0.0D);

        vp.addEdge("relateTo", vq, "weight", 0.1D);
        vp.addEdge("relateTo", vr, "weight", 0.2D);
        vp.addEdge("relateTo", vs, "weight", 0.3D);
        vp.addEdge("relateTo", vt, "weight", 0.4D);
        vp.addEdge("relateTo", vu, "weight", 0.5D);

        vq.addEdge("relateTo", vv, "weight", 0.6D);
        vr.addEdge("relateTo", vw, "weight", 0.7D);
        vs.addEdge("relateTo", vx, "weight", 0.8D);
        vt.addEdge("relateTo", vy, "weight", 0.9D);
        vu.addEdge("relateTo", vz, "weight", 1.0D);
    }

    @Test
    public void testCount() {
        CountRequest.Builder builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        CountRequest request = builder.build();

        long count = countAPI.post(request);
        Assert.assertEquals(8L, count);
    }

    @Test
    public void testCountWithContainsTraversed() {
        CountRequest.Builder builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(true);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        CountRequest request = builder.build();

        long count = countAPI.post(request);
        Assert.assertEquals(19L, count);
    }

    @Test
    public void testCountWithDirection() {
        CountRequest.Builder builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(true);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        CountRequest request = builder.build();

        long count = countAPI.post(request);
        Assert.assertEquals(19L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(8L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.IN);
        builder.steps().direction(Direction.IN);
        builder.steps().direction(Direction.IN);
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(3L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(true);
        builder.steps().direction(Direction.IN);
        builder.steps().direction(Direction.IN);
        builder.steps().direction(Direction.IN);
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(8L, count);
    }

    @Test
    public void testCountWithLabel() {
        CountRequest.Builder builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"));
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"));
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"));
        CountRequest request = builder.build();

        long count = countAPI.post(request);
        Assert.assertEquals(3L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(true);
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"));
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"));
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"));
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(8L, count);
    }

    @Test
    public void testCountWithProperties() {
        CountRequest.Builder builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"))
               .properties("time", "P.lt(\"2020-01-06\")");
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"))
               .properties("time", "P.lt(\"2020-01-06\")");
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"))
               .properties("time", "P.lt(\"2020-01-06\")");
        CountRequest request = builder.build();

        long count = countAPI.post(request);
        Assert.assertEquals(1L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(true);
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"))
               .properties("time", "P.lt(\"2020-01-06\")");
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"))
               .properties("time", "P.lt(\"2020-01-06\")");
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"))
               .properties("time", "P.lt(\"2020-01-06\")");
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(6L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"));
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"))
               .properties("time", "P.gt(\"2020-01-03\")");
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"));
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(1L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(true);
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"));
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"))
               .properties("time", "P.gt(\"2020-01-03\")");
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"));
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(4L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"));
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"));
        builder.steps().direction(Direction.OUT)
               .labels(ImmutableList.of("link"))
               .properties("time", "2020-01-07");
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(1L, count);
    }

    @Test
    public void testCountWithDegree() {
        CountRequest.Builder builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        CountRequest request = builder.build();

        long count = countAPI.post(request);
        Assert.assertEquals(8L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT).degree(1);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(3L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT).degree(2);
        builder.steps().direction(Direction.OUT);
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(4L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT).degree(4);
        builder.steps().direction(Direction.OUT);
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(7L, count);
    }

    @Test
    public void testCountWithSkipDegree() {
        CountRequest.Builder builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT);
        CountRequest request = builder.build();

        long count = countAPI.post(request);
        Assert.assertEquals(8L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT).degree(3).skipDegree(5);
        builder.steps().direction(Direction.OUT);
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(3L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT).degree(2).skipDegree(3);
        builder.steps().direction(Direction.OUT);
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(0L, count);

        builder = new CountRequest.Builder();
        builder.source("A").containsTraversed(false);
        builder.steps().direction(Direction.OUT);
        builder.steps().direction(Direction.OUT).degree(3).skipDegree(4);
        request = builder.build();

        count = countAPI.post(request);
        Assert.assertEquals(3L, count);
    }

    @Test
    public void testCountWithIllegalArgument() {
        CountRequest.Builder builder = new CountRequest.Builder();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            builder.source(null);
        }, e -> {
            Assert.assertContains("The source can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            builder.dedupSize(-5);
        }, e -> {
            Assert.assertContains("The dedup size must be >= 0 or == -1, " +
                                  "but got: ", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            builder.steps().degree(0);
        }, e -> {
            Assert.assertContains("Degree must be > 0 or == -1, but got: ",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            builder.steps().skipDegree(-3);
        }, e -> {
            Assert.assertContains("The skipped degree must be >= 0, but got",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            builder.steps().degree(5).skipDegree(3);
        }, e -> {
            Assert.assertContains("The skipped degree must be >= degree, ",
                                  e.getMessage());
        });

        CountRequest.Builder builder1 = new CountRequest.Builder();
        Assert.assertThrows(ServerException.class, () -> {
            builder1.source("A").containsTraversed(false);
            builder1.steps().properties(ImmutableMap.of("weight", 3.3D));
            countAPI.post(builder1.build());
        }, e -> {
            Assert.assertContains("The properties filter condition can be " +
                                  "set only if just set one edge label",
                                  e.getMessage());
        });

        CountRequest.Builder builder2 = new CountRequest.Builder();
        Assert.assertThrows(ServerException.class, () -> {
            builder2.source("A").containsTraversed(false);
            builder2.steps().labels(ImmutableList.of("link", "relateTo"))
                    .properties(ImmutableMap.of("weight", 3.3D));
            countAPI.post(builder2.build());
        }, e -> {
            Assert.assertContains("The properties filter condition can be " +
                                  "set only if just set one edge label",
                                  e.getMessage());
        });

        CountRequest.Builder builder3 = new CountRequest.Builder();
        builder3.source("A").containsTraversed(false);
        builder3.steps().labels(ImmutableList.of("link"))
                .properties(ImmutableMap.of("time", "2020-01-01"));
        countAPI.post(builder3.build());

        CountRequest.Builder builder4 = new CountRequest.Builder();
        Assert.assertThrows(ServerException.class, () -> {
            builder4.source("A").containsTraversed(false);
            builder4.steps().labels(ImmutableList.of("link"))
                    .properties(ImmutableMap.of("weight", 3.3D));
            countAPI.post(builder4.build());
        }, e -> {
            Assert.assertContains("does not match sort keys of edge label",
                                  e.getMessage());
        });
    }
}
