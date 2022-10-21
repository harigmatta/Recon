CREATE DATABASE IF NOT EXISTS ${hivevar:DBNAME};
 CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DBNAME}.CLOUD_ONPREM_RECON (
  SrcSystem STRING,
  Country STRING,
  SchemaName STRING,
  TableName STRING,
  SourcePartRecordCount BIGINT,
  StagingPartRecordCount BIGINT,
  TargetPartRecordCount BIGINT,
  SourcePartFileCount BIGINT,
  StagingPartFileCount BIGINT,
  TargetPartFileCount BIGINT,
  SourcePartSize BIGINT,
  StagingPartSize BIGINT,
  TargetPartSize BIGINT,
  SourcePath STRING,
  StagingPath STRING,
  TargetPath STRING,
  Status STRING
  ) Partitioned BY (partition_date STRING )
  STORED AS ORC
  LOCATION '${hivevar:LOCATION}/CLOUD_ONPREM_RECON';
  
