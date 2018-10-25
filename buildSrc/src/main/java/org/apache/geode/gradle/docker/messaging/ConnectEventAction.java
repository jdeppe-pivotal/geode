package org.apache.geode.gradle.docker.messaging;

import org.gradle.api.Action;
import org.gradle.internal.concurrent.ExecutorFactory;
import org.gradle.internal.remote.ObjectConnection;
import org.gradle.internal.remote.internal.ConnectCompletion;
import org.gradle.internal.remote.internal.hub.MessageHubBackedObjectConnection;

public class ConnectEventAction implements Action<ConnectCompletion> {

  private Action<ObjectConnection> action;
  private ExecutorFactory executorFactory;

  public ConnectEventAction(Action<ObjectConnection> action, ExecutorFactory executorFactory) {
    this.action = action;
    this.executorFactory = executorFactory;
  }

  @Override
  public void execute(ConnectCompletion connectCompletion) {
    action.execute(new MessageHubBackedObjectConnection(executorFactory, connectCompletion));
  }
}
