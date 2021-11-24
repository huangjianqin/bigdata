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
package org.kin.jraft.springboot.counter.server;

import java.io.Serializable;

/**
 * counter操作
 *
 * @author huangjianqin
 * @date 2021/11/14
 */
public class CounterOperation implements Serializable {
    private static final long serialVersionUID = -6597003954824547294L;

    /** Get value */
    public static final byte GET = 0x01;
    /** Increment and get value */
    public static final byte INCREMENT = 0x02;

    /** 操作类型 */
    private byte op;
    /** 值 */
    private long delta;

    public static CounterOperation createGet() {
        return new CounterOperation(GET);
    }

    public static CounterOperation createIncrement(long delta) {
        return new CounterOperation(INCREMENT, delta);
    }

    public CounterOperation(byte op) {
        this(op, 0);
    }

    public CounterOperation(byte op, long delta) {
        this.op = op;
        this.delta = delta;
    }

    public CounterOperation() {
    }

    //setter && getter
    public void setOp(byte op) {
        this.op = op;
    }

    public void setDelta(long delta) {
        this.delta = delta;
    }

    public byte getOp() {
        return op;
    }

    public long getDelta() {
        return delta;
    }
}
