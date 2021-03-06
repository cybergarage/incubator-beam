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
package org.apache.beam.runners.direct;

import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.IllegalMutationException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Collections;

/**
 * Tests for {@link ImmutabilityEnforcementFactory}.
 */
@RunWith(JUnit4.class)
public class ImmutabilityEnforcementFactoryTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private transient ImmutabilityEnforcementFactory factory;
  private transient BundleFactory bundleFactory;
  private transient PCollection<byte[]> pcollection;
  private transient AppliedPTransform<?, ?, ?> consumer;

  @Before
  public void setup() {
    factory = new ImmutabilityEnforcementFactory();
    bundleFactory = ImmutableListBundleFactory.create();
    TestPipeline p = TestPipeline.create();
    pcollection =
        p.apply(Create.of("foo".getBytes(), "spamhameggs".getBytes()))
            .apply(
                ParDo.of(
                    new DoFn<byte[], byte[]>() {
                      @Override
                      public void processElement(DoFn<byte[], byte[]>.ProcessContext c)
                          throws Exception {
                        c.element()[0] = 'b';
                      }
                    }));
    consumer = pcollection.apply(Count.<byte[]>globally()).getProducingTransformInternal();
  }

  @Test
  public void unchangedSucceeds() {
    WindowedValue<byte[]> element = WindowedValue.valueInGlobalWindow("bar".getBytes());
    CommittedBundle<byte[]> elements =
        bundleFactory.createRootBundle(pcollection).add(element).commit(Instant.now());

    ModelEnforcement<byte[]> enforcement = factory.forBundle(elements, consumer);
    enforcement.beforeElement(element);
    enforcement.afterElement(element);
    enforcement.afterFinish(
        elements,
        StepTransformResult.withoutHold(consumer).build(),
        Collections.<CommittedBundle<?>>emptyList());
  }

  @Test
  public void mutatedDuringProcessElementThrows() {
    WindowedValue<byte[]> element = WindowedValue.valueInGlobalWindow("bar".getBytes());
    CommittedBundle<byte[]> elements =
        bundleFactory.createRootBundle(pcollection).add(element).commit(Instant.now());

    ModelEnforcement<byte[]> enforcement = factory.forBundle(elements, consumer);
    enforcement.beforeElement(element);
    element.getValue()[0] = 'f';
    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage(consumer.getFullName());
    thrown.expectMessage("illegaly mutated");
    thrown.expectMessage("Input values must not be mutated");
    enforcement.afterElement(element);
    enforcement.afterFinish(
        elements,
        StepTransformResult.withoutHold(consumer).build(),
        Collections.<CommittedBundle<?>>emptyList());
  }

  @Test
  public void mutatedAfterProcessElementFails() {

    WindowedValue<byte[]> element = WindowedValue.valueInGlobalWindow("bar".getBytes());
    CommittedBundle<byte[]> elements =
        bundleFactory.createRootBundle(pcollection).add(element).commit(Instant.now());

    ModelEnforcement<byte[]> enforcement = factory.forBundle(elements, consumer);
    enforcement.beforeElement(element);
    enforcement.afterElement(element);

    element.getValue()[0] = 'f';
    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage(consumer.getFullName());
    thrown.expectMessage("illegaly mutated");
    thrown.expectMessage("Input values must not be mutated");
    enforcement.afterFinish(
        elements,
        StepTransformResult.withoutHold(consumer).build(),
        Collections.<CommittedBundle<?>>emptyList());
  }
}
