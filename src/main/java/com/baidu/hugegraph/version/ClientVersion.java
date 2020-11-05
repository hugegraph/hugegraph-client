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

package com.baidu.hugegraph.version;

import com.baidu.hugegraph.util.VersionUtil;
import com.baidu.hugegraph.util.VersionUtil.Version;

public class ClientVersion {

    static {
        // Check versions of the dependency packages
        ClientVersion.check();
    }

    public static final String NAME = "hugegraph-client";

    public static final Version VERSION = Version.of(ClientVersion.class);

    public static final void check() {
        // Check version of hugegraph-common
        VersionUtil.check(CommonVersion.VERSION, "1.8", "1.9",
                          CommonVersion.NAME);
    }
}
