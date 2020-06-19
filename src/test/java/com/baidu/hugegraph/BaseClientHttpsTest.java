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

package com.baidu.hugegraph;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.driver.HugeClient;

public class BaseClientHttpsTest {

    private static final String BASE_URL = "https://127.0.0.1:8443";
    private static final String GRAPH = "hugegraph";
    private static final String PASSWORD = "";
    private static final String PROTOCOL = "https";
    private static final int TIMEOUT = 10;
    private static final String TRUST_STORE_FILE = "src/test/resources/cacerts.jks";
    private static final String TRUST_STORE_PASSWORD = "changeit";
    private static final String USERNAME = "";
    private static final int maxConnsPerRoute = 10;
    private static final int maxCoons = 10;

    @Test
    public void testHttpsClient1() {
        HugeClient hugeClient;
        hugeClient = new HugeClient(BASE_URL, GRAPH, USERNAME, PASSWORD, TIMEOUT,
                                    PROTOCOL, maxCoons, maxConnsPerRoute, TRUST_STORE_FILE,
                                    TRUST_STORE_PASSWORD);
        Assert.assertNotNull("Not openend client", hugeClient);
    }

    @Test
    public void testHttpsClient2() {
        HugeClient hugeClient;
        hugeClient = new HugeClient(BASE_URL, GRAPH, USERNAME, PASSWORD, TIMEOUT,
                                    PROTOCOL, TRUST_STORE_FILE, TRUST_STORE_PASSWORD);
        Assert.assertNotNull("Not openend client", hugeClient);
    }
}
