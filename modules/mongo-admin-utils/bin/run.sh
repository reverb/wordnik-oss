#!/bin/bash

export CLASSPATH=$(echo target/*.jar target/lib/*.jar | tr \  :)
#JAVA_DEBUG_OPTIONS="-Xdebug -Xrunjdwp:transport=dt_socket,address=8005,server=y,suspend=n "
JAVA_CONFIG_OPTIONS="-Xms500m -Xmx1000m -XX:NewSize=200m -XX:MaxNewSize=200m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:PermSize=100m -XX:MaxPermSize=100m"
export JAVA_OPTS="-Duser.timezone=GMT ${JAVA_CONFIG_OPTIONS} ${JAVA_DEBUG_OPTIONS} "

java $WORDNIK_OPTS $JAVA_CONFIG_OPTIONS $JAVA_OPTS -cp $CLASSPATH "$@"
