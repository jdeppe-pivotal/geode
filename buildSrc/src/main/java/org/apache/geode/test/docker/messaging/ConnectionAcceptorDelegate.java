package org.apache.geode.test.docker.messaging;

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
        try {
          for (Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces(); nics.hasMoreElements(); ) {
            NetworkInterface nic = nics.nextElement();
            if (nic.isUp() && !nic.isLoopback()) {
              nic.getInterfaceAddresses().forEach(i -> {
                InetAddress address = i.getAddress();
                if (!address.isLinkLocalAddress()) {
                  remoteAddresses.add(address);
                }
              });
            }
          }
        } catch (SocketException e) {
          e.printStackTrace();
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
