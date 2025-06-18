/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


CREATE GRAPH IF NOT EXISTS loop_detection_graph (
  Vertex person (
    id BIGINT ID,
    name VARCHAR
  ),
  Edge knows (
    srcId BIGINT SOURCE ID,
    targetId BIGINT DESTINATION ID,
    weight DOUBLE
  )
) WITH (
  storeType='rocksdb',
  shardCount = 1
);

INSERT INTO loop_detection_graph.person(id, name)
SELECT 1, 'Alice'
UNION ALL
SELECT 2, 'Bob'
UNION ALL
SELECT 3, 'Charlie'
UNION ALL
SELECT 4, 'David';

INSERT INTO loop_detection_graph.knows(srcId, targetId, weight)
SELECT 1, 2, 0.8
UNION ALL
SELECT 2, 3, 0.6
UNION ALL
SELECT 3, 4, 0.9
UNION ALL
SELECT 4, 1, 0.7;  -- 创建环路的边