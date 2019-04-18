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

import com.baidu.hugegraph.api.graph.GraphAPI;
import com.baidu.hugegraph.api.traverser.structure.Ranks;
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

    public Ranks post(Request request) {
        RestResult result = this.client.post(this.path(), request);
        return result.readObject(Ranks.class);
    }

    public static class Request {

        @JsonProperty("source")
        private String source;
        @JsonProperty("label")
        private String label;
        @JsonProperty("alpha")
        private double alpha = Traverser.DEFAULT_ALPHA;
        @JsonProperty("degree")
        public long degree = Traverser.DEFAULT_DEGREE;
        @JsonProperty("limit")
        private long limit = Traverser.DEFAULT_LIMIT;
        @JsonProperty("max_depth")
        private int maxDepth = Traverser.DEFAULT_MAX_DEPTH;
        @JsonProperty("with_label")
        private WithLabel withLabel = WithLabel.BOTH_LABEL;
        @JsonProperty("sorted")
        private boolean sorted = true;

        public static Builder builder() {
            return new Builder();
        }

        @Override
        public String toString() {
            return String.format("Request{source=%s,label=%s,alpha=%s," +
                                 "degree=%s,limit=%s,maxDepth=%s," +
                                 "withLabel=%s,sorted=%s}",
                                 this.source, this.label, this.alpha,
                                 this.degree, this.limit, this.maxDepth,
                                 this.withLabel, this.sorted);
        }

        public enum WithLabel {
            SAME_LABEL,
            OTHER_LABEL,
            BOTH_LABEL
        }

        public static class Builder {

            private Request request;

            private Builder() {
                this.request = new Request();
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

            public Builder limit(long limit) {
                TraversersAPI.checkLimit(limit);
                this.request.limit = limit;
                return this;
            }

            public Builder maxDepth(int maxDepth) {
                TraversersAPI.checkPositive(maxDepth,
                                            "max depth of rank request " +
                                            "for personal rank");
                this.request.maxDepth = maxDepth;
                return this;
            }

            public Builder withLabel(WithLabel withLabel) {
                this.request.withLabel = withLabel;
                return this;
            }

            public Builder sorted(boolean sorted) {
                this.request.sorted = sorted;
                return this;
            }

            public Request build() {
                E.checkArgument(this.request.source != null,
                                "Source vertex can't be null");
                E.checkArgument(this.request.label != null,
                                "The label of rank request " +
                                "for personal rank can't be null");
                TraversersAPI.checkAlpha(this.request.alpha);
                TraversersAPI.checkDegree(this.request.degree);
                TraversersAPI.checkLimit(this.request.limit);
                TraversersAPI.checkPositive(this.request.maxDepth,
                                            "max depth of rank request " +
                                            "for personal rank");
                return this.request;
            }
        }
    }
}
