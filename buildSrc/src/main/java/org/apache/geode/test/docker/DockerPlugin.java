package org.apache.geode.test.docker;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.concurrent.Executor;

import org.gradle.StartParameter;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.internal.DocumentationRegistry;
import org.gradle.api.internal.project.DefaultProject;
import org.gradle.api.internal.tasks.testing.JvmTestExecutionSpec;
import org.gradle.api.internal.tasks.testing.TestExecuter;
import org.gradle.api.internal.tasks.testing.detection.DefaultTestExecuter;
import org.gradle.api.tasks.testing.Test;
import org.gradle.initialization.BuildCancellationToken;
import org.gradle.initialization.DefaultBuildCancellationToken;
import org.gradle.internal.concurrent.DefaultExecutorFactory;
import org.gradle.internal.concurrent.ExecutorFactory;
import org.gradle.internal.operations.BuildOperationExecutor;
import org.gradle.internal.service.ServiceRegistry;
import org.gradle.internal.time.Clock;
import org.gradle.internal.work.WorkerLeaseRegistry;
import org.gradle.process.internal.JavaExecHandleFactory;
import org.gradle.process.internal.worker.DefaultWorkerProcessFactory;

public class DockerPlugin implements Plugin<Project> {

  @Override
  public void apply(Project project) {
    for (Iterator<Task> i = project.getTasks().iterator(); i.hasNext(); ) {
      Task task = i.next();
      if (task instanceof Test) {
        configureTest(project, task);
      }
    }
  }

  private void configureTest(Project project, Task test) {
    test.getExtensions().create("docker", DockerPluginExtension.class);

    test.doFirst(x -> {
      DockerPluginExtension
          extension =
          (DockerPluginExtension) x.getExtensions().getByName("docker");

      ServiceRegistry registry = get(x, "getServices");

      DefaultWorkerProcessFactory processFactory = get(x, "getProcessBuilderFactory");

      ExecutorFactory executorFactory = new DefaultExecutorFactory();
      Executor executor = executorFactory.create("Docker container link");
      BuildCancellationToken buildCancellationToken = new DefaultBuildCancellationToken();
      DockerJavaExecHandleFactory
          execHandleFactory =
          new DockerJavaExecHandleFactory(extension, ((DefaultProject) project).getFileResolver(),
              executor, buildCancellationToken);

      setExecHandler(processFactory, execHandleFactory);

      DefaultTestExecuter testExecuter = new DefaultTestExecuter(
          processFactory,
          get(x, "getActorFactory"),
          get(x, "getModuleRegistry"),
          registry.get(WorkerLeaseRegistry.class),
          registry.get(BuildOperationExecutor.class),
          registry.get(StartParameter.class).getMaxWorkerCount(),
          registry.get(Clock.class),
          registry.get(DocumentationRegistry.class),
          get(x, "getFilter"));

      setTestExecuter((Test) x, testExecuter);
    });
  }

  private void setTestExecuter(Test test, TestExecuter<JvmTestExecutionSpec> executer) {
    try {
      Method m = test.getClass().getDeclaredMethod("testExecuter", TestExecuter.class);
      m.setAccessible(true);
      m.invoke(test, executer);
      m.setAccessible(false);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      e.printStackTrace();
      throw new GradleException("Unable to set test executer", e);
    }
  }

  private void setExecHandler(DefaultWorkerProcessFactory processFactory,
                              JavaExecHandleFactory execHandleFactory) {
    try {
      Field f = processFactory.getClass().getDeclaredField("execHandleFactory");
      f.setAccessible(true);

      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(f, f.getModifiers() & ~Modifier.FINAL);

      f.set(processFactory, execHandleFactory);
      f.setAccessible(false);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      e.printStackTrace();
      throw new GradleException("Unable to set exec handler", e);
    }
  }

  private <T> T get(Object test, String methodName) {
    Method getterMethod = null;
    Class<?> candidate = test.getClass();
    while (getterMethod == null && candidate != Object.class) {
      try {
        getterMethod = candidate.getDeclaredMethod(methodName);
      } catch (NoSuchMethodException e) {
        candidate = candidate.getSuperclass();
      }
    }

    if (getterMethod == null) {
      throw new GradleException("Unable to find method " + methodName);
    }

    try {
      boolean accessibility = getterMethod.isAccessible();
      getterMethod.setAccessible(true);
      Object registry = getterMethod.invoke(test);
      getterMethod.setAccessible(accessibility);
      return (T) registry;
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new GradleException("Unable to call " + methodName);
    }
  }

}
