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
