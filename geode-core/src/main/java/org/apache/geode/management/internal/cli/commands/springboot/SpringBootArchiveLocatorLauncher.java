package org.apache.geode.management.internal.cli.commands.springboot;

import org.springframework.boot.loader.JarLauncher;

public class SpringBootArchiveLocatorLauncher extends JarLauncher {

  public SpringBootArchiveLocatorLauncher() {
    super();
  }

  public static void main(String[] args) throws Exception {
    new SpringBootArchiveLocatorLauncher().launch(args);
  }

  @Override
  public String getMainClass() {
    return "org.apache.geode.distributed.LocatorLauncher";
  }
}
