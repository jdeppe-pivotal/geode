package org.apache.geode.scratch;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.AttachContainerResultCallback;
import com.github.dockerjava.netty.NettyDockerCmdExecFactory;

public class Scratch {

  public static void main(String[] args) throws Exception {

    DockerClient dockerClient = DockerClientBuilder
        .getInstance(DefaultDockerClientConfig.createDefaultConfigBuilder())
        .withDockerCmdExecFactory(new NettyDockerCmdExecFactory())
        .build();

    String snippet = "hello world";

    CreateContainerResponse container = dockerClient.createContainerCmd("openjdk:geode")
        .withCmd("sh", "-c", "read line && echo $line; X=5; while [ $X -gt 0 ]; do date; sleep 1; X=$(expr $X - 1); done")
        .withTty(false)
        .withStdinOpen(true)
        .exec();

    dockerClient.startContainerCmd(container.getId()).exec();

    AttachContainerResultCallback callback = new AttachContainerResultCallback() {
      @Override
      public void onNext(Frame frame) {
        System.out.println("OUTPUT: " + frame.toString());
        super.onNext(frame);
      }
    };

    PipedOutputStream out = new PipedOutputStream();
    PipedInputStream in = new PipedInputStream(out);

    dockerClient.attachContainerCmd(container.getId())
        .withStdErr(true)
        .withStdOut(true)
        .withFollowStream(true)
        .withStdIn(in)
        .exec(callback);

    out.write((snippet + "\n").getBytes());
    out.flush();

    callback.awaitCompletion();
    callback.close();

    out.close();
    in.close();

    dockerClient.close();
  }
}
