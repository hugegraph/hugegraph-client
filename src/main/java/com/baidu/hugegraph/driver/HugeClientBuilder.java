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

package com.baidu.hugegraph.driver;

public class HugeClientBuilder {
    private final String graph;
    private final int maxConns;
    private final int maxConnsPerRoute;
    private final String password;
    private final String protocol;
    private final int timeout;
    private final String trustStoreFile;
    private final String trustStorePassword;
    private final String url;
    private final String username;

    public HugeClientBuilder() {
        this(new Builder());
    }

    private HugeClientBuilder(Builder builder) {
        this.graph = builder.graph;
        this.maxConns = builder.maxConns;
        this.maxConnsPerRoute = builder.maxConnsPerRoute;
        this.password = builder.password;
        this.protocol = builder.protocol;
        this.timeout = builder.timeout;
        this.trustStoreFile = builder.trustStoreFile;
        this.trustStorePassword = builder.trustStorePassword;
        this.url = builder.url;
        this.username = builder.username;
    }

    public String getGraph() {
        return graph;
    }

    public int getMaxConns() {
        return maxConns;
    }

    public int getMaxConnsPerRoute() {
        return maxConnsPerRoute;
    }

    public String getPassword() {
        return password;
    }

    public String getProtocol() {
        return protocol;
    }

    public int getTimeout() {
        return timeout;
    }

    public String getTrustStoreFile() {
        return trustStoreFile;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public Builder newBuilder() {
        return new Builder(this);
    }

    public static final class Builder {
        private String graph;
        private int maxConns;
        private int maxConnsPerRoute;
        private String password;
        private String protocol;
        private int timeout;
        private String trustStoreFile;
        private String trustStorePassword;
        private String url;
        private String username;

        public Builder() {
            graph = "hugegraph";
            protocol = "http";
            url = "http://127.0.0.1:8080";
            username = "";
            password = "";
        }

        Builder(HugeClientBuilder hugeClientBuilder) {
            this.graph = hugeClientBuilder.graph;
            this.maxConns = hugeClientBuilder.maxConns;
            this.maxConnsPerRoute = hugeClientBuilder.maxConnsPerRoute;
            this.password = hugeClientBuilder.password;
            this.protocol = hugeClientBuilder.protocol;
            this.timeout = hugeClientBuilder.timeout;
            this.trustStoreFile = hugeClientBuilder.trustStoreFile;
            this.trustStorePassword = hugeClientBuilder.trustStorePassword;
            this.url = hugeClientBuilder.url;
            this.username = hugeClientBuilder.username;

        }

        public Builder setGraph(String graph) {
            this.graph = graph;
            return this;
        }

        public Builder setMaxConns(int maxConns) {
            this.maxConns = maxConns;
            return this;
        }

        public Builder setMaxConnsPerRoute(int maxConnsPerRoute) {
            this.maxConnsPerRoute = maxConnsPerRoute;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setProtocol(String protocol) {
            this.protocol = protocol;
            return this;
        }

        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setTrustStoreFile(String trustStoreFile) {
            this.trustStoreFile = trustStoreFile;
            return this;
        }

        public Builder setTrustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public HugeClientBuilder build() {
            return new HugeClientBuilder(this);
        }
    }
}
