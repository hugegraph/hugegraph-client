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

import com.baidu.hugegraph.util.E;

public class HugeClientBuilder {

    private static final int CPUS = Runtime.getRuntime().availableProcessors();
    private static final int DEFAULT_TIMEOUT = 20;
    private static final int DEFAULT_MAX_CONNS = 4 * CPUS;
    private static final int DEFAULT_MAX_CONNS_PER_ROUTE = 2 * CPUS;
    private static final String DEFAULT_HTTP_PREFIX = "http";
    private static final int DEFAULT_IDLE_TIME = 30;

    private String graph;
    private int idleTime;
    private int maxConns;
    private int maxConnsPerRoute;
    private String password;
    private String protocol;
    private int timeout;
    private String trustStoreFile;
    private String trustStorePassword;
    private String url;
    private String username;

    public HugeClientBuilder() {
        this.username = "";
        this.password = "";
        this.timeout = DEFAULT_TIMEOUT;
        this.maxConns = DEFAULT_MAX_CONNS;
        this.maxConnsPerRoute = DEFAULT_MAX_CONNS_PER_ROUTE;
        this.protocol = DEFAULT_HTTP_PREFIX;
        this.trustStoreFile = "";
        this.trustStorePassword = "";
        this.idleTime = DEFAULT_IDLE_TIME;
    }

    public HugeClientBuilder(String url, String graph) {
        E.checkArgument(url != null && !url.isEmpty(),
                        "Expect a string value as the url " +
                                "parameter argument, but got: %s", url);
        E.checkArgument(graph != null && !graph.isEmpty(),
                        "Expect a string value as the graph name " +
                                "parameter argument, but got: %s", graph);
        this.url = url;
        this.graph = graph;
        this.username = "";
        this.password = "";
        this.timeout = DEFAULT_TIMEOUT;
        this.maxConns = DEFAULT_MAX_CONNS;
        this.maxConnsPerRoute = DEFAULT_MAX_CONNS_PER_ROUTE;
        this.protocol = DEFAULT_HTTP_PREFIX;
        this.trustStoreFile = "";
        this.trustStorePassword = "";
        this.idleTime = DEFAULT_IDLE_TIME;
    }

    public HugeClient build() {
        E.checkArgument(this.url == null,
                        "The url parameter is invalid and cannot be null");
        E.checkArgument(this.graph == null,
                        "The graph name parameter is invalid and cannot be null");

        return new HugeClient().create(this);
    }

    public HugeClientBuilder configGraph(String graph) {
        this.graph = graph;
        return this;
    }

    public HugeClientBuilder configIdleTime(int idleTime) {
        this.idleTime = idleTime;
        return this;
    }

    public HugeClientBuilder configPool(int maxConns, int maxConnsPerRoute) {
        if (maxConns == 0) {
            maxConns = DEFAULT_MAX_CONNS;
        }
        if (maxConnsPerRoute == 0) {
            maxConnsPerRoute = DEFAULT_MAX_CONNS_PER_ROUTE;
        }
        this.maxConns = maxConns;
        this.maxConnsPerRoute = maxConnsPerRoute;
        return this;
    }

    public HugeClientBuilder configSSL(String protocol, String trustStoreFile,
                                       String trustStorePassword) {
        if (protocol == null) {
            protocol = DEFAULT_HTTP_PREFIX;
        }
        this.protocol = protocol;
        this.trustStoreFile = trustStoreFile;
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public HugeClientBuilder configTimeout(int timeout) {
        if (timeout == 0) {
            timeout = DEFAULT_TIMEOUT;
        }
        this.timeout = timeout;
        return this;
    }

    public HugeClientBuilder configUrl(String url) {
        this.url = url;
        return this;
    }

    public HugeClientBuilder configUser(String username, String password) {
        if (username == null) {
            username = "";
        }
        if (password == null) {
            password = "";
        }
        this.username = username;
        this.password = password;

        return this;
    }

    public String graph() {
        return graph;
    }

    public int idleTime() {
        return idleTime;
    }

    public int maxConns() {
        return maxConns;
    }

    public int maxConnsPerRoute() {
        return maxConnsPerRoute;
    }

    public String password() {
        return password;
    }

    public String protocol() {
        return protocol;
    }

    public int timeout() {
        return timeout;
    }

    public String trustStoreFile() {
        return trustStoreFile;
    }

    public String trustStorePassword() {
        return trustStorePassword;
    }

    public String url() {
        return url;
    }

    public String username() {
        return username;
    }
}
