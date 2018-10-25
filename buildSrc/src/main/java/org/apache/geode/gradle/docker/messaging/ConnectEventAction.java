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
