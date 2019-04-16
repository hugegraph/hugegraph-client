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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.api.graph.GraphAPI;
import com.baidu.hugegraph.client.RestClient;
import com.baidu.hugegraph.rest.RestResult;
import com.baidu.hugegraph.structure.constant.Traverser;
import com.baidu.hugegraph.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PersonalRankAPI extends TraversersAPI {

    public PersonalRankAPI(RestClient client, String graph) {
        super(client, graph);
    }

    @Override
    protected String type() {
        return "personalrank";
    }

    @SuppressWarnings("unchecked")
    public Map<Object, Double> post(RankRequest request) {
        RestResult result = this.client.post(this.path(), request);
        return result.readObject(Map.class);
    }

    public static class RankRequest {

        @JsonProperty("source")
        private String source;
        @JsonProperty("label")
        private String label;
        @JsonProperty("alpha")
        private double alpha;
        @JsonProperty("degree")
        public long degree = Traverser.DEFAULT_DEGREE;
        @JsonProperty("max_depth")
        private int maxDepth;
        @JsonProperty("sorted")
        private boolean sorted = true;

        @Override
        public String toString() {
            return String.format("RankRequest{source=%s,label=%s," +
                                 "alpha=%s,degree=%s,maxDepth=%s,sorted=%s}",
                                 this.source, this.label, this.alpha,
                                 this.degree, this.maxDepth, this.sorted);
        }

        public static class Builder {

            private RankRequest request;

            public Builder() {
                this.request = new RankRequest();
            }

            public Builder source(Object source) {
                this.request.source = GraphAPI.formatVertexId(source);
                return this;
            }

            public Builder label(String label) {
                E.checkArgument(label != null, "The label of rank request " +
                                "for personal rank can't be null");
                this.request.label = label;
                return this;
            }

            public Builder alpha(double alpha) {
                TraversersAPI.checkAlpha(alpha);
                this.request.alpha = alpha;
                return this;
            }

            public Builder degree(long degree) {
                TraversersAPI.checkDegree(degree);
                this.request.degree = degree;
                return this;
            }

            public Builder maxDepth(int maxDepth) {
                TraversersAPI.checkPositive(maxDepth,
                                            "max depth of rank request " +
                                            "for personal rank");
                this.request.maxDepth = maxDepth;
                return this;
            }

            public Builder sorted(boolean sorted) {
                this.request.sorted = sorted;
                return this;
            }

            public RankRequest build() {
                E.checkArgument(this.request.source != null,
                                "Source vertex can't be null");
                E.checkArgument(this.request.label != null,
                                "The label of rank request " +
                                "for personal rank can't be null");
                TraversersAPI.checkAlpha(this.request.alpha);
                TraversersAPI.checkDegree(this.request.degree);
                TraversersAPI.checkPositive(this.request.maxDepth,
                                            "max depth of rank request " +
                                            "for personal rank");
                return this.request;
            }
        }
    }
}
