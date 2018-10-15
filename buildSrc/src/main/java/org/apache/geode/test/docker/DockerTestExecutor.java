package org.apache.geode.test.docker;

import org.gradle.api.internal.tasks.testing.JvmTestExecutionSpec;
import org.gradle.api.internal.tasks.testing.TestExecuter;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;

public class DockerTestExecutor implements TestExecuter<JvmTestExecutionSpec> {

  @Override
  public void execute(JvmTestExecutionSpec testExecutionSpec,
                      TestResultProcessor testResultProcessor) {
    throw new RuntimeException("BANG!");
  }

  @Override
  public void stopNow() {

  }
}
