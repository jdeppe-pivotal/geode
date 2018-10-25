package org.apache.geode.gradle.docker;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;
import com.github.dockerjava.api.model.WaitResponse;
import com.github.dockerjava.core.command.AttachContainerResultCallback;
import com.github.dockerjava.core.command.WaitContainerResultCallback;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

public class DockerProcess extends Process {

  private static final Logger LOGGER = Logging.getLogger(DockerProcess.class);
  private final DockerClient dockerClient;
  private final String containerId;
//  private final Closure afterContainerStop;

  private final PipedOutputStream stdInWriteStream = new PipedOutputStream();
  private final PipedInputStream stdOutReadStream = new PipedInputStream();
  private final PipedInputStream stdErrReadStream = new PipedInputStream();
  private final PipedInputStream stdInReadStream = new PipedInputStream(stdInWriteStream);
  private final PipedOutputStream stdOutWriteStream = new PipedOutputStream(stdOutReadStream);
  private final PipedOutputStream stdErrWriteStream = new PipedOutputStream(stdErrReadStream);

  private final CountDownLatch finished = new CountDownLatch(1);
  private AtomicInteger exitCode = new AtomicInteger();
  private final AttachContainerResultCallback
      attachContainerResultCallback =
      new AttachContainerResultCallback() {
        @Override
        public void onNext(Frame frame) {
          try {
            if (frame.getStreamType().equals(StreamType.STDOUT)) {
              stdOutWriteStream.write(frame.getPayload());
            } else if (frame.getStreamType().equals(StreamType.STDERR)) {
              stdErrWriteStream.write(frame.getPayload());
            }
          } catch (Exception e) {
            LOGGER.error("Error while writing to stream:", e);
          }
          super.onNext(frame);
        }
      };

  private final WaitContainerResultCallback
      waitContainerResultCallback =
      new WaitContainerResultCallback() {
        @Override
        public void onNext(WaitResponse waitResponse) {
          exitCode.set(waitResponse.getStatusCode());
          try {
            attachContainerResultCallback.close();
            attachContainerResultCallback.awaitCompletion();
            stdOutWriteStream.close();
            stdErrWriteStream.close();
          } catch (Exception e) {
            LOGGER.debug("Error by detaching streams", e);
          } finally {
            try {
              dockerClient.removeContainerCmd(containerId).exec();
              dockerClient.close();
            } catch (Exception e) {
              LOGGER.debug("Exception thrown when removing container", e);
            } finally {
              finished.countDown();
            }
          }
        }
      };

  public DockerProcess(final DockerClient dockerClient, final String containerId) throws Exception {
    this.dockerClient = dockerClient;
    this.containerId = containerId;
//    this.afterContainerStop = afterContainerStop;
    attachStreams();
    dockerClient.waitContainerCmd(containerId).exec(waitContainerResultCallback);
  }

  private void attachStreams() throws Exception {
    dockerClient.attachContainerCmd(containerId)
        .withFollowStream(true)
        .withStdOut(true)
        .withStdErr(true)
        .withStdIn(stdInReadStream)
        .exec(attachContainerResultCallback);
    if (!attachContainerResultCallback.awaitStarted(10, TimeUnit.SECONDS)) {
      LOGGER.warn("Not attached to container " + containerId + " within 10secs");
      throw new RuntimeException("Not attached to container " + containerId + " within 10secs");
    }
  }

  @Override
  public OutputStream getOutputStream() {
    return stdInWriteStream;
  }

  @Override
  public InputStream getInputStream() {
    return stdOutReadStream;
  }

  @Override
  public InputStream getErrorStream() {
    return stdErrReadStream;
  }

  @Override
  public int waitFor() throws InterruptedException {
    finished.await();
    return exitCode.get();
  }

  @Override
  public int exitValue() {
    if (finished.getCount() > 0) {
      throw new IllegalThreadStateException("docker process still running");
    }
    return exitCode.get();
  }

  @Override
  public void destroy() {
    dockerClient.killContainerCmd(containerId).exec();
  }

  @Override
  public String toString() {
    return "Container " + containerId + " on " + dockerClient.toString();
  }
}
