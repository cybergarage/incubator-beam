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
package org.apache.beam.runners.torrent;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

public class TestTorrentRunner extends PipelineRunner<TorrentRunnerResult> {

  private TorrentRunner delegate;

  private TestTorrentRunner(TorrentPipelineOptions options) {
  }

  public static TestTorrentRunner fromOptions(PipelineOptions options) {
    TorrentPipelineOptions TorrentOptions = PipelineOptionsValidator.validate(TorrentPipelineOptions.class, options);
    return new TestTorrentRunner(TorrentOptions);
  }

  public static TestTorrentRunner create(boolean streaming) {
    TorrentPipelineOptions TorrentOptions = PipelineOptionsFactory.as(TorrentPipelineOptions.class);
    TorrentOptions.setRunner(TestTorrentRunner.class);
    TorrentOptions.setStreaming(streaming);
    return TestTorrentRunner.fromOptions(TorrentOptions);
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput>
      OutputT apply(PTransform<InputT,OutputT> transform, InputT input) {
    return delegate.apply(transform, input);
  }

  @Override
  public TorrentRunnerResult run(Pipeline pipeline) {
    return null;
  }

  public PipelineOptions getPipelineOptions() {
    return null;
  }
}