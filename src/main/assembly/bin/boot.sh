#!/bin/sh


PRG_NAME=zongfen-statmange
WORK_DIR=$(cd `dirname $0`; pwd)/../
LOG_DIR="$WORK_DIR"/logs

JAVA=java
JAVA_OPTS="-DZF_HOME=${ZF_HOME} -Djava.io.tmpdir=${WORK_DIR}/tmp -Xms256m -Xmx512m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:-CMSConcurrentMTEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -Dwork.dir=${WORK_DIR}"
CLASS_PATH=" -classpath "$(echo ${WORK_DIR}/lib/*.jar|sed 's/ /:/g')
CLASS=com.aotain.statmange.Main
PNAME=zf-statmange

if [ ! -d "${LOG_DIR}" ]; then
  mkdir -p ${LOG_DIR}
fi

echo "ZF_HOME:$ZF_HOME"

cd $WORK_DIR

case "$1" in

  start)
    exec "$JAVA" $JAVA_OPTS $CLASS_PATH $CLASS ${WORK_DIR} ${PNAME} |tee ${LOG_DIR}/${PRG_NAME}.log
	echo "${PRG_NAME} is running,pid=$!."
    echo "${PRG_NAME} start----> "`date  '+%Y-%m-%d %H:%M:%S'` >>${LOG_DIR}/${PRG_NAME}.out
    ;;
  *)
    echo "Usage: boot.sh {start} "
    ;;

esac

exit 0
