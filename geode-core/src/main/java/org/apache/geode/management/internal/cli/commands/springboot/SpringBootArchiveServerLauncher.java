package org.apache.geode.management.internal.cli.commands.springboot;

import org.springframework.boot.loader.JarLauncher;

public class SpringBootArchiveServerLauncher extends JarLauncher {

  public SpringBootArchiveServerLauncher() {
    super();
  }

  public static void main(String[] args) throws Exception {
    new SpringBootArchiveServerLauncher().launch(args);
  }

  @Override
  public String getMainClass() {
    return "org.apache.geode.distributed.ServerLauncher";
  }
}
