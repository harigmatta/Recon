#Parameters to be passed to the script

#<keytab> <kerberos principal> <src_system> <country> <abfs_container <hdfs_parent_dir> sri_open_staging_dir>
# <sri_non_open_staging_dir> <storage_staging_dir> <databases> <tables> <partitionColumnMap> <partition> <reconDB>
# <reconTable> <ctrl_file_path>

KERBEROS_KEYTAB_PATH=$1
KERBEROS_PRINCIPAL=$2
src_system=$3
country=$4
abfs_container=$5
hdfs_parent_dir=$6
sri_open_staging_dir=$7
sri_non_open_staging_dir=$8
storage_staging_dir=$9
databases=$10
tables=$11
partitionColumnMap=$12
partition=$13
reconDB=$14
reconTable=$15
ctrl_file_path=$16

#export KB5CCNAME="/tmp/krb5cc_%{uid}"
#kinit -kt ${KERBEROS_KEYTAB_PATH} ${KERBEROS_PRINCIPAL}


export SPARK_MAJOR_VERSION=2
export DATA_COPY_HOME=`dirname $PWD`
export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Djava.security.krb5.conf=/hadoopfs/fs1/onprem_hdfs_config/krb5.conf"

DATA_COPY_LOGS_DIR="/data/edmpcloudfw/SDM/SIT/deployment/cdp_data_copy/logs" #"<archivelocation>"
dt=`date +%Y-%m-%d-%H-%M-%S`
AppName="CloudRecon_${src_system}_${country}"

echo "SourceSystem:${src_system}" >> ${DATA_COPY_LOGS_DIR}/${AppName}_${dt}.log
echo "Country:${country}" >> ${DATA_COPY_LOGS_DIR}/${AppName}_${dt}.log
echo "Appname:${AppName}" >> ${DATA_COPY_LOGS_DIR}/${AppName}_${dt}.log

/data/onprem/hdp/spark/bin/spark-submit --class com.cloudera.ps.CloudReconcile \
--master yarn \
--name ${AppName} \
--deploy-mode cluster \
--driver-memory 10G \
--executor-memory 4G \
--executor-cores 4 --num-executors 8 --queue "sgz1-apps-auirrb_au_all_all_batchuser_fullaccess_sit-dev" \
--conf "spark.driver.cores=10" \
--principal ${KERBEROS_PRINCIPAL} \
--keytab ${KERBEROS_KEYTAB_PATH} \
--conf "spark.hadoop.hadoop.security.credential.provider.path=jceks://hdfs/tmp/sit/hdata/cdpstoragedev.jceks" \
--conf spark.yarn.maxAppAttempts=2 \
$DATA_COPY_HOME/lib/CloudReconcile-1.1-SNAPSHOT.jar \
$src_system $country $abfs_container $hdfs_parent_dir $sri_open_staging_dir $sri_non_open_staging_dir \
$storage_staging_dir $databases \"$tables\" $partitionColumnMap $partition $reconDB $reconTable \
$ctrl_file_path 2>&1 >/dev/null|tee -a ${DATA_COPY_LOGS_DIR}/${AppName}_${dt}.log
#| grep "Submit.*application" >> ${DATA_COPY_LOGS_DIR}/${AppName}_${dt}.log

#chmod 775 ${DATA_COPY_LOGS_DIR}/${AppName}_${dt}.log
