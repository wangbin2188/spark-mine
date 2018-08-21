#如果需要传参数：./hadoop_compress.sh '2018-07-01 15'
export JAVA_HOME=/usr/local/java/jdk
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:$JAVA_HOME/lib/:$JRE_HOME/lib/
export PATH=$JAVA_HOME/bin:$PATH

if [ ! -n "$1" ] ;then
    work_date=`date -d "-1 hours" `
else
    work_date="$1"
fi
clock_hour=`date  +"%Y%m%d %H" -d  "${work_date} "`
hour_1=`date  +"%H" -d "${work_date}"`
input_day=`date  +"%Y%m%d" -d  "${work_date} "`
output_time=`date +"%Y%m%d_%H" -d "${work_date}"`
spark_input=/tmp/logs/shtermuser/origin/${output_time}*/*
spark_output=/tmp/logs/shtermuser/finish/${output_time}
hive_input=/tmp/logs/shtermuser/finish/${output_time}/
hive_output=/user/hive/warehouse/bglogs.db/app_base_log/day=${input_day}/

#对sparkstreaming输出目录进行格式化，作为下一步的输入
renamePathFun(){
for ((i=0;i<60;i=i+10))
do
cur_time=`date +"%s" -d "${clock_hour} ${i} minutes"`
input_1=/tmp/logs/shtermuser/origin/'-'${cur_time}'000'
output_1=/tmp/logs/shtermuser/origin/${output_time}-${i}
hadoop fs -mv ${input_1} ${output_1}
done
}

#将当前hour的所有文件使用spark压缩处理##############################################################
compressFun(){
#判断spark输出目录是否存在，存在则删除
hadoop fs -test -e ${spark_output}
if [ $? -eq 0 ];then
    hadoop fs -rm -r ${spark_output}
fi

spark-submit \
--class MergeCompress  \
--master yarn  --executor-memory 10g \
/data/bglogs/sparkstreaming/UserTagProcess-2.jar ${spark_input} ${spark_output} 
hadoop fs -rm -r ${spark_input}
}

#将压缩后的文件重命名后move到hive表hdfs目录#########################################################
moveFun(){
#判断目标目录是否不存在，不存在则创建,并给hive表增加分区
hadoop fs -test -e ${hive_output}
if [ $? -eq 1 ];then
    hadoop fs -mkdir ${hive_output}
    hive -e "use bglogs;alter table app_base_log add partition(day='${input_day}')"
fi

renameFun ${hive_input}
hadoop fs -mv ${hive_input}/* ${hive_output}
}

#文件进入hdfs目录前进行重命名#######################################################################
renameFun(){
input=$1
hadoop fs -ls ${input} | awk '{print $8}' >/data/bglogs/sparkstreaming/files0.txt
cat  /data/bglogs/sparkstreaming/files0.txt | while read line
do
	_path=`echo $line | sed "s/part/${hour_1}-part/"`
	hadoop fs -mv $line $_path
done
}
renamePathFun
compressFun
moveFun
