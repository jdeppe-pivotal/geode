package org.apache.geode.test.docker;

import java.util.HashMap;
import java.util.Map;

import groovy.lang.Closure;

public class DockerPluginExtension {
  private String image;
  private Map<String, String> volumes = new HashMap<>();
  private String user;
  private Closure beforeContainerCreate;
  private Closure afterContainerCreate;
  private Closure beforeContainerStart;
  private Closure afterContainerStart;
  private Closure afterContainerStop;

  public DockerPluginExtension() {
//    afterContainerStop = () -> { };
  }

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }

  public Map<String, String> getVolumes() {
    return volumes;
  }

  public void setVolumes(Map<String, String> volumes) {
    this.volumes = volumes;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public Closure getBeforeContainerCreate() {
    return beforeContainerCreate;
  }

  public void setBeforeContainerCreate(Closure beforeContainerCreate) {
    this.beforeContainerCreate = beforeContainerCreate;
  }

  public Closure getAfterContainerCreate() {
    return afterContainerCreate;
  }

  public void setAfterContainerCreate(Closure afterContainerCreate) {
    this.afterContainerCreate = afterContainerCreate;
  }

  public Closure getBeforeContainerStart() {
    return beforeContainerStart;
  }

  public void setBeforeContainerStart(Closure beforeContainerStart) {
    this.beforeContainerStart = beforeContainerStart;
  }

  public Closure getAfterContainerStart() {
    return afterContainerStart;
  }

  public void setAfterContainerStart(Closure afterContainerStart) {
    this.afterContainerStart = afterContainerStart;
  }

  public Closure getAfterContainerStop() {
    return afterContainerStop;
  }

  public void setAfterContainerStop(Closure afterContainerStop) {
    this.afterContainerStop = afterContainerStop;
  }
}
