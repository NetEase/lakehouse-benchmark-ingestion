/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.benchmark.ingestion.params;

import com.netease.arctic.benchmark.ingestion.params.table.ArcticParameters;
import com.netease.arctic.benchmark.ingestion.params.table.HudiParameters;
import com.netease.arctic.benchmark.ingestion.params.table.IcebergParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParameterUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ParameterUtil.class);

  public static Class<?> getParamsClass(String type) {
    switch (type) {
      case "arctic":
        return ArcticParameters.class;
      case "iceberg":
        return IcebergParameters.class;
      case "hudi":
        return HudiParameters.class;
      default:
        LOG.error("get type {} parameter class fail", type);
        return null;
    }
  }
}
