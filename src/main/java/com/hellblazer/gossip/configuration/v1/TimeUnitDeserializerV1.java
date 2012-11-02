/** 
 * (C) Copyright 2010 Hal Hildebrand, All Rights Reserved
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */

package com.hellblazer.gossip.configuration.v1;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.deser.std.FromStringDeserializer;

/**
 * @author hhildebrand
 * 
 */
public class TimeUnitDeserializerV1 extends FromStringDeserializer<TimeUnit> {
    public TimeUnitDeserializerV1() {
        super(TimeUnit.class);
    }

    @Override
    protected TimeUnit _deserialize(String value, DeserializationContext ctxt)
                                                                              throws IOException {
        return TimeUnit.valueOf(value.toUpperCase());
    }
}
