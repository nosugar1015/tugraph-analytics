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

package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.type.primitive.BooleanType;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Description(name = "cycle_detect", description = "Detects cycles from a given start vertex")
public class LoopDetection implements AlgorithmUserFunction<Object, ObjectRow> {

    // 定义消息类型常量
    private static final int PATH_MSG = 0;  // 路径传播消息
    private static final int MARK_MSG = 1;  // 标记环消息
    
    private AlgorithmRuntimeContext<Object, ObjectRow> context;
    private Object startVertexId;
    private String vertexType = null;
    private int maxIteration = 20;

    @Override
    public void init(AlgorithmRuntimeContext<Object, ObjectRow> context, Object[] parameters) {
        this.context = context;
        if (parameters.length > 3) {
            throw new IllegalArgumentException(
                "Only support zero to three arguments. Usage: cycle_detect(start_vertex_id, [vertex_type], [max_iteration])");
        }
        
        // 修复类型问题：统一转为Long
        if (parameters.length >= 1) {
            if (parameters[0] instanceof Long) {
                this.startVertexId = parameters[0];
            } else if (parameters[0] instanceof Integer) {
                this.startVertexId = ((Integer) parameters[0]).longValue();
            } else {
                throw new IllegalArgumentException("Start vertex id parameter should be long or integer type");
            }
        }
        
        if (parameters.length >= 2) {
            if (!(parameters[1] instanceof String)) {
                throw new IllegalArgumentException("Vertex type parameter should be string type");
            }
            vertexType = (String) parameters[1];
        }
        if (parameters.length >= 3) {
            if (!(parameters[2] instanceof Integer)) {
                throw new IllegalArgumentException("Max iteration parameter should be integer type");
            }
            maxIteration = (Integer) parameters[2];
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<ObjectRow> messages) {
        updatedValues.ifPresent(vertex::setValue);
        
        // 顶点类型过滤
        if (vertexType != null && !vertexType.equals(vertex.getLabel())) {
            return;
        }

        if (context.getCurrentIterationId() == 1L) {
            // 起始顶点初始化
            if (vertex.getId().equals(startVertexId)) {
                for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                    // 发送空路径+发送者ID
                    context.sendMessage(edge.getTargetId(), 
                        ObjectRow.create(PATH_MSG, new ArrayList<>(), vertex.getId()));
                }
            }
        } else if (context.getCurrentIterationId() <= maxIteration) {
            // 处理所有消息
            while (messages.hasNext()) {
                ObjectRow msg = messages.next();
                int msgType = (Integer) msg.getField(0, IntegerType.INSTANCE);
                
                if (msgType == MARK_MSG) {
                    // 标记当前顶点在环中
                    context.updateVertexValue(ObjectRow.create(true));
                } 
                else if (msgType == PATH_MSG) {
                    List<Object> path = (List<Object>) msg.getField(1, null);
                    Object senderId = msg.getField(2, LongType.INSTANCE);
                    
                    // 检查环：当前顶点是否已在路径中
                    if (path.contains(vertex.getId())) {
                        // 提取环上所有顶点：从首次出现位置到末尾
                        int firstIndex = path.indexOf(vertex.getId());
                        Set<Object> cycleVertices = new HashSet<>(
                            path.subList(firstIndex, path.size())
                        );
                        // 包括当前发送者（环闭合点）
                        cycleVertices.add(senderId);
                        
                        // 标记环上所有顶点
                        for (Object vid : cycleVertices) {
                            context.sendMessage(vid, ObjectRow.create(MARK_MSG, null, null));
                        }
                    } else {
                        // 更新路径：添加当前发送者ID
                        List<Object> newPath = new ArrayList<>(path);
                        newPath.add(senderId);
                        
                        // 继续传播
                        for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                            context.sendMessage(edge.getTargetId(), 
                                ObjectRow.create(PATH_MSG, newPath, vertex.getId()));
                        }
                    }
                }
            }
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> newValue) {
        if (newValue.isPresent()) {
            Boolean hasCycle = (Boolean) newValue.get().getField(0, BooleanType.INSTANCE);
            if (hasCycle != null && hasCycle) {
                context.take(ObjectRow.create(vertex.getId(), true));
            }
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField("has_cycle", BooleanType.INSTANCE, false)
        );
    }
}