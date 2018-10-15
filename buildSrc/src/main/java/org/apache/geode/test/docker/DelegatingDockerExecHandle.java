package org.apache.geode.test.docker;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.gradle.process.ExecResult;
import org.gradle.process.internal.DefaultExecHandle;
import org.gradle.process.internal.ExecHandle;
import org.gradle.process.internal.ExecHandleListener;
import org.gradle.process.internal.ExecHandleState;
import org.gradle.process.internal.ProcessSettings;

public class DelegatingDockerExecHandle implements ExecHandle, ProcessSettings {

  private DefaultExecHandle delegate;

  public DelegatingDockerExecHandle(DefaultExecHandle delegate) {
    this.delegate = delegate;
  }

  @Override
  public File getDirectory() {
    return delegate.getDirectory();
  }

  @Override
  public String getCommand() {
    return delegate.getCommand();
  }

  @Override
  public List<String> getArguments() {
    return delegate.getArguments();
  }

  @Override
  public Map<String, String> getEnvironment() {
    return delegate.getEnvironment();
  }

  @Override
  public boolean getRedirectErrorStream() {
    return delegate.getRedirectErrorStream();
  }

  @Override
  public ExecHandle start() {
    return delegate.start();
  }

  @Override
  public ExecHandleState getState() {
    return delegate.getState();
  }

  @Override
  public void abort() {
    delegate.abort();
  }

  @Override
  public ExecResult waitForFinish() {
    return delegate.waitForFinish();
  }

  @Override
  public void addListener(ExecHandleListener listener) {
    delegate.addListener(listener);
  }

  @Override
  public void removeListener(ExecHandleListener listener) {
    delegate.removeListener(listener);
  }
}
