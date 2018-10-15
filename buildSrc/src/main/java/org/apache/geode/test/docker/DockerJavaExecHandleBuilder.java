package org.apache.geode.test.docker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.gradle.api.internal.file.FileResolver;
import org.gradle.initialization.BuildCancellationToken;
import org.gradle.process.internal.AbstractExecHandleBuilder;
import org.gradle.process.internal.ExecHandle;
import org.gradle.process.internal.ExecHandleListener;
import org.gradle.process.internal.JavaExecHandleBuilder;
import org.gradle.process.internal.StreamsHandler;
import org.gradle.process.internal.streams.OutputStreamsForwarder;

public class DockerJavaExecHandleBuilder extends JavaExecHandleBuilder {

  private final List<ExecHandleListener> listeners = new ArrayList<>();
  private StreamsHandler streamsHandler;
  private boolean redirectErrorStream;
  private int timeoutMillis = Integer.MAX_VALUE;
  private boolean daemon;
  private Executor executor;
  private BuildCancellationToken buildCancellationToken;
  private DockerPluginExtension extension;

  public DockerJavaExecHandleBuilder(DockerPluginExtension extension,
                                     FileResolver fileResolver,
                                     Executor executor,
                                     BuildCancellationToken buildCancellationToken) {
    super(fileResolver, executor, buildCancellationToken);
    this.extension = extension;
    this.executor = executor;
    this.buildCancellationToken = buildCancellationToken;
  }

  @Override
  public ExecHandle build() {
    try {
      return new DockerExecHandle(
          extension,
          getDisplayName(),
          getWorkingDir(),
          "java",
          getAllArguments(),
          getActualEnvironment(),
          getEffectiveStreamsHandler(),
          getInputHandler(),
          listeners,
          redirectErrorStream,
          timeoutMillis,
          daemon,
          executor,
          buildCancellationToken
      );
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public AbstractExecHandleBuilder listener(ExecHandleListener listener) {
    this.listeners.add(listener);
    return super.listener(listener);
  }

  @Override
  public AbstractExecHandleBuilder streamsHandler(StreamsHandler streamsHandler) {
    this.streamsHandler = streamsHandler;
    return super.streamsHandler(streamsHandler);
  }

  @Override
  public AbstractExecHandleBuilder redirectErrorStream() {
    this.redirectErrorStream = true;
    return super.redirectErrorStream();
  }

  @Override
  public AbstractExecHandleBuilder setTimeout(int timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
    return super.setTimeout(timeoutMillis);
  }

  public AbstractExecHandleBuilder setDaemon(boolean daemon) {
    this.daemon = daemon;
    super.daemon = daemon;
    return this;
  }

  private StreamsHandler getEffectiveStreamsHandler() {
    StreamsHandler effectiveHandler;
    if (this.streamsHandler != null) {
      effectiveHandler = this.streamsHandler;
    } else {
      boolean shouldReadErrorStream = !redirectErrorStream;
      effectiveHandler = new OutputStreamsForwarder(getStandardOutput(), getErrorOutput(), shouldReadErrorStream);
    }
    return effectiveHandler;
  }
}
