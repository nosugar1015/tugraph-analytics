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

-- 设置窗口为 -1 表示全局分析
set geaflow.dsl.window.size = -1;

-- 定义图结构
CREATE GRAPH IF NOT EXISTS g1 (
	Vertex person (
	  id bigint ID,
	  name varchar,
	  age int
	),
	Edge knows (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID,
	  weight double
	)
) WITH (
	storeType='rocksdb',
	shardCount = 1
);

-- 加载顶点数据
CREATE TABLE modern_vertex (
  id varchar,
  type varchar,
  name varchar,
  other varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/test_vertex.txt'
);

-- 加载边数据
CREATE TABLE modern_edge (
  srcId bigint,
  targetId bigint,
  type varchar,
  weight double
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/test_edge.txt'
);

-- 插入顶点
INSERT INTO g1.person
SELECT cast(id as bigint), name, cast(other as int) as age
FROM modern_vertex WHERE type = 'person';

-- 插入边
INSERT INTO g1.knows
SELECT srcId, targetId, weight
FROM modern_edge WHERE type = 'knows';

-- 创建结果输出表
CREATE TABLE tbl_result (
  id bigint,
  has_cycle boolean
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

-- 切换图上下文
USE GRAPH g1;

-- 执行环路检测算法
INSERT INTO tbl_result
CALL cycle_detect(1, 'person', 20) YIELD (id, has_cycle)
RETURN id, has_cycle;