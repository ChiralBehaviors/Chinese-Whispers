/*
 * (C) Copyright 2014 Chiral Behaviors, All Rights Reserved
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

package com.hellblazer.gossip.configuration;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.hellblazer.utils.fd.impl.AdaptiveFailureDetectorFactory;
import com.hellblazer.utils.fd.impl.PhiFailureDetectorFactory;
import com.hellblazer.utils.fd.impl.TimedFailureDetectorFactory;

/**
 * @author hhildebrand
 * 
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
               @Type(value = AdaptiveFailureDetectorFactory.class, name = "adaptiveFailureDetectorFactory"),
               @Type(value = PhiFailureDetectorFactory.class, name = "phiFailureDetectorFactory"),
               @Type(value = TimedFailureDetectorFactory.class, name = "timedFailureDetectorFactory"), })
public abstract class FdFactoryMixin {
}