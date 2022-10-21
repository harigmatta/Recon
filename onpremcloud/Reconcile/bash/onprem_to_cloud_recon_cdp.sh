#Parameters to be passed to the script
#<keytab> <kerberos principal> <src system> <country> <HDFS_Parent_Dir> <ABFS_Prefix> <consumed_tables> <partition> <database> <db_part_map> <recon_db> <recon_table>
KERBEROS_KEYTAB_PATH=$1
KERBEROS_PRINCIPAL=$2
SRCSYSTEM=$3
COUNTRY=$4
HDFS_PARENT_DIR=$5
ABFS_PREFIX=$6
CONSUMED_TABLES=$7
PARTTIION=$8
DATABASE=$9
DB_PART_MAP=${10}
RECON_DB=${11}
RECON_TABLE=${12}

echo "KERBEROS_KEYTAB_PATH="${KERBEROS_KEYTAB_PATH}
echo "KERBEROS_PRINCIPAL="${KERBEROS_PRINCIPAL}
echo "SRCSYSTEM="${SRCSYSTEM}
echo "COUNTRY="${COUNTRY}
echo "HDFS_PARENT_DIR="${HDFS_PARENT_DIR}
echo "ABFS_PREFIX="${KERBEROS_KEYTAB_PATH}
echo "CONSUMED_TABLES="${CONSUMED_TABLES}
echo "PARTTIION="${PARTTIION}
echo "DATABASE="${DATABASE}
echo "DB_PART_MAP="${DB_PART_MAP}
echo "RECON_DB="${RECON_DB}
echo "RECON_TABLE="${RECON_TABLE}

kinit -kt ${KERBEROS_KEYTAB_PATH} ${KERBEROS_PRINCIPAL}

export SPARK_MAJOR_VERSION=2

export SDM_HOME=/CTRLFW/SOURCG/SDM/SIT/deployment
export SDM_VERSION=1.3.1

DATA_COPY_LOGS_DIR="/data/edmpcloudfw/SDM/SIT/deployment/cdp_data_copy/logs" #"<archivelocation>"
dt=`date +%Y-%m-%d-%H-%M-%S`
AppName="RECON_$SRCSYSTEM_$COUNTRY"

echo "SRCSYSTEM:$SRCSYSTEM" >> ${DATA_COPY_LOGS_DIR}/${AppName}_${dt}.log
echo "COUNTRY:$COUNTRY" >> ${DATA_COPY_LOGS_DIR}/${AppName}_${dt}.log
echo "Appname:${AppName}" >> ${DATA_COPY_LOGS_DIR}/${AppName}_${dt}.log

spark-submit --class com.cloudera.ps.ReconcileAllTables \
 --master yarn \
 --deploy-mode cluster \
 --driver-memory 10G \
 --executor-memory 4G \
 --executor-cores 4 --num-executors 8 --queue default \
 --conf "spark.driver.cores=10" \
 --principal ${KERBEROS_PRINCIPAL} \
 --keytab ${KERBEROS_KEYTAB_PATH} \
 --conf "spark.hadoop.hadoop.security.credential.provider.path=jceks://hdfs/secrets/cdpstoragesit.jceks" \
 --conf spark.yarn.maxAppAttempts=1 \
$SDM_HOME/recon_cdp/Reconcile-1.6-SNAPSHOT.jar $SRCSYSTEM $COUNTRY $HDFS_PARENT_DIR $ABFS_PREFIX $CONSUMED_TABLES $PARTTIION $DATABASE $DB_PART_MAP $RECON_DB $RECON_TABLE 2>&1 >/dev/null|tee -a ${DATA_COPY_LOGS_DIR}/${AppName}_${dt}.log


if [ ${PIPESTATUS[0]} -eq 0 ]
then
  exit 0
else
  exit 1
fi