package org.apache.geode.test.docker;

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
