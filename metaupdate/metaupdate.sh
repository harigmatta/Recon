#Parameters to be passed to the script

#<keytab> <kerberos principal> <src path> <tkn path> <cloud path> <partition> <recon type> <tgt db> <tgt table>

KERBEROS_KEYTAB_PATH=$1

KERBEROS_PRINCIPAL=$2

SOURCE_SYSTEM=$3

COUNTRY=$4

STATEMENTS=$5

STATUS_DIR=$6

PARTITION=$7

kinit -kt ${KERBEROS_KEYTAB_PATH} ${KERBEROS_PRINCIPAL}

export SPARK_MAJOR_VERSION=2
export DATA_COPY_HOME=`dirname $PWD`

META_LOGS_DIR="<archivelocation>"
AppName="METAUPDATE_${SOURCE_SYSTEM}_${COUNTRY}"
dt=`date +%Y-%m-%d-%H-%M-%S`

echo "Source system:${SOURCE_SYSTEM}" >> ${META_LOGS_DIR}/${AppName}_${dt}.log
echo "Country:${COUNTRY}" >> ${META_LOGS_DIR}/${AppName}_${dt}.log
echo "Partition:${PARTITION}">> ${META_LOGS_DIR}/${AppName}_${dt}.log
echo "Appname:${AppName}" >> ${META_LOGS_DIR}/${AppName}_${dt}.log

spark-submit --class com.cloudera.ps.AddPartitions \
--master yarn \
--name ${AppName} \
--deploy-mode cluster \
--driver-memory 5G \
--executor-memory 1G \
--executor-cores 1 --num-executors 1 --queue default \
--conf "spark.driver.cores=2" \
--principal ${KERBEROS_PRINCIPAL} \
--keytab ${KERBEROS_KEYTAB_PATH} \
--conf "spark.hadoop.hadoop.security.credential.provider.path=jceks://hdfs/hdp/apps/clouddatacopy/cdpstoragedatacopy.jceks" \
--conf spark.yarn.maxAppAttempts=2 \
$DATA_COPY_HOME/lib/AddPartitions-1.0-SNAPSHOT.jar $SOURCE_SYSTEM $COUNTRY $STATEMENTS $STATUS_DIR $PARTITION 2>&1 >/dev/null >> ${META_LOGS_DIR}/${AppName}_${dt}.log
