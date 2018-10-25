/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.gradle.docker;

import java.util.concurrent.Executor;

import org.gradle.api.internal.file.FileResolver;
import org.gradle.initialization.BuildCancellationToken;
import org.gradle.process.internal.JavaExecHandleBuilder;
import org.gradle.process.internal.JavaExecHandleFactory;

public class DockerJavaExecHandleFactory implements JavaExecHandleFactory {

  private FileResolver fileResolver;
  private Executor executor;
  private BuildCancellationToken buildCancellationToken;
  private DockerPluginExtension extension;

  public DockerJavaExecHandleFactory(DockerPluginExtension extension, FileResolver fileResolver, Executor executor,
                                     BuildCancellationToken buildCancellationToken) {
    this.extension = extension;
    this.fileResolver = fileResolver;
    this.executor = executor;
    this.buildCancellationToken = buildCancellationToken;
  }

  @Override
  public JavaExecHandleBuilder newJavaExec() {
    return new DockerJavaExecHandleBuilder(extension, fileResolver, executor, buildCancellationToken);
  }
}
