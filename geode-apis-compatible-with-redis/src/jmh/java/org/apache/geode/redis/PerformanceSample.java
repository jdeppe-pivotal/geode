/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

public class PerformanceSample {

  @State(Scope.Thread)
  public static class BenchmarkState {
    List<Integer> list;

    @Setup(Level.Trial)
    public void initialize() {
      Random rand = new Random();

      list = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        list.add(rand.nextInt());
      }
    }
  }

  @Benchmark
  public void benchmark1(BenchmarkState state, Blackhole bh) {
    List<Integer> list = state.list;

    for (int i = 0; i < 1000; i++) {
      bh.consume(list.get(i));
    }
  }

  @Test
  public void launchBenchmark() throws Exception {
    Options opt = new OptionsBuilder()
        // Specify which benchmarks to run.
        // You can be more specific if you'd like to run only one benchmark per test.
        .include(this.getClass().getName() + ".*")
        // Set the following options as needed
        .mode(Mode.AverageTime)
        .timeUnit(TimeUnit.MICROSECONDS)
        .warmupTime(TimeValue.seconds(1))
        .warmupIterations(2)
        .measurementTime(TimeValue.seconds(1))
        .measurementIterations(2)
        .threads(2)
        .forks(1)
        .shouldFailOnError(true)
        .shouldDoGC(true)
        // .jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining")
        // .addProfiler(SafepointsProfiler.class)
        // .addProfiler(GCProfiler.class)
        // .addProfiler(HotspotRuntimeProfiler.class)
        // .addProfiler(HotspotMemoryProfiler.class)
        // .addProfiler(StackProfiler.class)
        .build();

    new Runner(opt).run();
  }
}
