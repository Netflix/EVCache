. /etc/profile.d/netflix_environment.sh

let GB=`free -m | grep '^Mem:' | awk '{print $2}'`
HEAP_MB=$(( $GB - 2048 ))
NEW_MB=$(( $HEAP_MB * 6 / 10 ))

GCLOG=/logs/tomcat/gc.log
CATALINA_PID=/logs/tomcat/catalina.pid
JRE_HOME=/apps/java
JAVA_HOME=$JRE_HOME
export TZ=GMT
if [ "$1" == "start" ]; then
JAVA_OPTS=" \
 -verbosegc \
 -XX:+PrintGCDetails \
 -XX:+PrintGCTimeStamps \
 -Xloggc:$GCLOG \
 -Xmx${HEAP_MB}m \
 -Xms${HEAP_MB}m \
 -Xss1024k \
 -XX:NewSize=${NEW_MB}m \
 -XX:SurvivorRatio=8 \
 -XX:+UseConcMarkSweepGC \
 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false "

#####
#
#
# Separate options for stopping Tomcat. It is important to not use the same start options, at least for
# gclog file name. Using the same gclog file for stopping clobbers the gclogs for the running tomcat.
#
else
JAVA_OPTS=""
fi
