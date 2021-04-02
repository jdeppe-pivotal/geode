#!/usr/bin/env bash

set -x

#NI=/Library/Java/JavaVirtualMachines/graalvm-ce-java8-21.0.0.2/Contents/Home/bin/native-image
NI=/Library/Java/JavaVirtualMachines/graalvm-ce-java11-21.0.0.2/Contents/Home/bin/native-image

case $1 in
  server)
    BUILD_CLASS=org.apache.geode.distributed.ServerLauncher
    IMAGE=gedis-server
    ;;
  locator)
    BUILD_CLASS=org.apache.geode.distributed.LocatorLauncher
    IMAGE=gedis-locator
    ;;
  *)
    echo "Unknown option - valid args are 'locator' or 'server'"
    exit 1
    ;;
esac

$NI -cp './geode-assembly/build/install/apache-geode/lib/*' \
  ${BUILD_CLASS} ${IMAGE} \
  --initialize-at-build-time=org.apache.logging.log4j.util.StackLocator,\
org.apache.logging.log4j.util.PropertySource\$Util,\
org.apache.logging.log4j.util.LoaderUtil,\
org.apache.logging.log4j.util.PropertiesUtil \
  --initialize-at-run-time=\
com.sun.jmx,\
io.netty,\
javax.management.remote.rmi.RMIConnector,\
javax.xml.parsers.FactoryFinder,\
jdk.management.jfr.SettingDescriptorInfo,\
org.apache.geode.alerting.log4j.internal.impl.AlertAppender,\
org.apache.geode.logging.log4j.internal.impl.LogWriterAppender,\
org.apache.logging.log4j,\
org.slf4j,\
springfox.documentation,\
javax.net.ssl.HttpsURLConnection,javax.net.ssl.HttpsURLConnection\$DefaultHostnameVerifier \
  --no-fallback \
  --report-unsupported-elements-at-runtime \
  --allow-incomplete-classpath \
  --features=org.graalvm.home.HomeFinderFeature \
  -H:ConfigurationFileDirectories=${PWD}/graal-config/${IMAGE} \
  -H:IncludeResourceBundles=joptsimple.ExceptionMessages
  
