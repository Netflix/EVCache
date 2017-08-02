. /etc/profile.d/netflix_environment.sh

if [ "$NETFLIX_ENVIRONMENT" == "persistence_test" ]; then
	export NETFLIX_ENVIRONMENT="test"
fi
if [ "$NETFLIX_ENVIRONMENT" == "persistence_prod" ]; then
	export NETFLIX_ENVIRONMENT="prod"
fi


GCLOG=/logs/tomcat/gc.log
CATALINA_PID=/logs/tomcat/catalina.pid
JRE_HOME=/apps/java
JAVA_HOME=$JRE_HOME
export TZ=GMT
if [ "$1" == "start" ]; then
JAVA_OPTS=" \
 -verbosegc \
 -XX:+PrintGCDetails \
 -XX:+PrintGCDateStamps \
 -Xloggc:$GCLOG \
 -Xmx512m \
 -Xms128m \
 -Xss1024k \
 -XX:SurvivorRatio=8 \
 -XX:+UseConcMarkSweepGC \
 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dnetflix.environment=${NETFLIX_ENVIRONMENT} -Dnetflix.appinfo.name=${NETFLIX_APP}"

#####
#
#
# Separate options for stopping Tomcat. It is important to not use the same start options, at least for
# gclog file name. Using the same gclog file for stopping clobbers the gclogs for the running tomcat.
#
else
JAVA_OPTS=""
fi

