package org.apache.geode.test.docker.messaging;

import org.gradle.api.Action;
import org.gradle.internal.concurrent.ExecutorFactory;
import org.gradle.internal.remote.ConnectionAcceptor;
import org.gradle.internal.remote.MessagingServer;
import org.gradle.internal.remote.ObjectConnection;
import org.gradle.internal.remote.internal.IncomingConnector;

public class MessageServer implements MessagingServer {

  private IncomingConnector incomingConnector;
  private ExecutorFactory executorFactory;

  public MessageServer(IncomingConnector incomingConnector, ExecutorFactory executorFactory) {
    this.incomingConnector = incomingConnector;
    this.executorFactory = executorFactory;
  }

  @Override
  public ConnectionAcceptor accept(Action<ObjectConnection> action) {
    return new ConnectionAcceptorDelegate(
        incomingConnector.accept(new ConnectEventAction(action, executorFactory), true));
  }
}
