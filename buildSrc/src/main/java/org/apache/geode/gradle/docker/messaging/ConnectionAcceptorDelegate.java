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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.gradle.internal.remote.Address;
import org.gradle.internal.remote.ConnectionAcceptor;
import org.gradle.internal.remote.internal.inet.MultiChoiceAddress;

public class ConnectionAcceptorDelegate implements ConnectionAcceptor {

  private ConnectionAcceptor delegate;
  private MultiChoiceAddress address;

  public ConnectionAcceptorDelegate(ConnectionAcceptor delegate) {
    this.delegate = delegate;
  }

  @Override
  public Address getAddress() {
    synchronized (delegate) {
      if (address == null) {
        List<InetAddress> remoteAddresses = new ArrayList<>();
        Enumeration<NetworkInterface> nics;
        try {
          nics = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
          e.printStackTrace();
          return null;
        }
        for (; nics.hasMoreElements(); ) {
          NetworkInterface nic = nics.nextElement();
          try {
            if (nic.isUp() && !nic.isLoopback()) {
              nic.getInterfaceAddresses().forEach(i -> {
                InetAddress address = i.getAddress();
                if (!address.isLinkLocalAddress()) {
                  remoteAddresses.add(address);
                }
              });
            }
          } catch (SocketException e) {
            // Ignored because this nic probably disappeared
          }
        }
        MultiChoiceAddress originalAddress = (MultiChoiceAddress) delegate.getAddress();
        address = new MultiChoiceAddress(originalAddress.getCanonicalAddress(),
            originalAddress.getPort(), remoteAddresses);
      }
    }

    return address;
  }

  @Override
  public void requestStop() {
    delegate.requestStop();
  }

  @Override
  public void stop() {
    delegate.stop();
  }
}
