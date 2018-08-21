export JAVA_HOME=/usr/local/java/jdk
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:$JAVA_HOME/lib/:$JRE_HOME/lib/
export PATH=$JAVA_HOME/bin:$PATH

tagFun(){

min_time=`date  +"%Y%m%d" -d  "$1"`

min_time_stamp=`date  +"%s" -d "${min_time}"`
max_time_stamp=`date  +"%s" -d "${min_time} 1 day"`

spark-submit \
--supervise \
--master yarn \
--class UserTag \
--conf spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC \
--name "user tag" \
--executor-memory 2g \
/data/bglogs/sparkstreaming/UserTagProcess-2.jar  ${min_time_stamp}'000' ${max_time_stamp}'000'
}


addressFun(){
spark-submit \
--supervise \
--master yarn \
--class UserAddress \
--conf spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC \
--name "user address" \
--executor-memory 4g \
/data/bglogs/sparkstreaming/UserTagProcess-1.jar  $1
}

firstAndLastFun(){
spark-submit \
--supervise \
--master yarn \
--class UserFirstAndLast \
--conf spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC \
--name "user last" \
--executor-memory 4g \
/data/bglogs/sparkstreaming/UserTagProcess-1.jar $1
}

#########################################################
# package  UserTagProcess-1.jar
# contains class (UserAddress,UserTag,UserFirstAndLast)
# excuting ResolveAddress =>addressFun

today=`date  +"%Y%m%d" -d  "${date} -1 days"`
if [ ! -n "$1" ] ;then
    start="${today}"
    end="${today}"
elif [ ! -n "$2" ] ;then
    start="$1"
    end="${today}"
else
    start="$1"
    end="$2"
fi

while [ ${start} -le ${end} ]
do
day=`date  +"%Y%m%d" -d  "${start}"`
echo ${day}
tagFun ${day}
addressFun ${day}
firstAndLastFun ${day}
start=`date  +"%Y%m%d" -d  "${start} 1 days" `
done
