/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.trino.udf.scalar;

import com.dtstack.trino.uitl.IPAddressUtils;
import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.airlift.slice.Slices.utf8Slice;

/**
 * @author chuixue
 * @create 2022-11-02 16:48
 * @description
 **/
public class IPtoLocation {
    private static IPAddressUtils ip = new IPAddressUtils();

    static {
        ip.init();
    }

    public static String evaluate(String value) {

        return ip.getIPLocation(value).getCountry();
    }

    @Description("IPToLocation(String value1)")
    @ScalarFunction("IPToLocation")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice evaluate(@SqlType(StandardTypes.VARCHAR) Slice value) {

        return utf8Slice(ip.getIPLocation(value.toStringUtf8()).getCountry());
    }

    //析构函数
    public void finalize() {
        ip.clean();
    }


    public static void main(String[] args) {
        String ip = "117.149.23.58";
        System.out.println("结果：" + evaluate(ip));
    }
}
