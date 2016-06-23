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
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import com.google.common.base.Joiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A {@link PipelineRunner} that executes the operations in the
 * pipeline by first translating them to a Flink Plan and then executing them either locally
 * or on a Torrent cluster, depending on the configuration.
 * <p>
 */
public class TorrentRunner extends PipelineRunner<TorrentRunnerResult> {

  private static final Logger LOG = LoggerFactory.getLogger(TorrentRunner.class);

  /**
   * Provided options.
   */
  private final TorrentPipelineOptions options;

  /**
   * Construct a runner from the provided options.
   *
   * @param options Properties which configure the runner.
   * @return The newly created runner.
   */
  public static TorrentRunner fromOptions(PipelineOptions options) {
    TorrentPipelineOptions torrentOptions =
        PipelineOptionsValidator.validate(TorrentPipelineOptions.class, options);
    return new TorrentRunner(torrentOptions);
  }

  private TorrentRunner(TorrentPipelineOptions options) {
    this.options = options;
  }

  @Override
  public TorrentRunnerResult run(Pipeline pipeline) {
    LOG.info("Executing pipeline using TorrentRunner.");

    // TODO Implement Runner
    //LOG.info("Execution finished in {} msecs", result.getNetRuntime());

    return new TorrentRunnerResult(null, 0);
  }


  @Override
  public <Output extends POutput, Input extends PInput> Output apply(
      PTransform<Input, Output> transform, Input input) {
    return super.apply(transform, input);
  }

  /////////////////////////////////////////////////////////////////////////////

  @Override
  public String toString() {
    return "TorrentRunner#" + hashCode();
  }
}
