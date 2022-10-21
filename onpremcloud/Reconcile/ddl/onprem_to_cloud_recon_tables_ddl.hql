 CREATE EXTERNAL TABLE INC_ONPREM_CLOUD_RECON (
  SrcSystem STRING,
  Country STRING,
  SchemaName STRING,
  TableName STRING,
  SourcePartRecordCount BIGINT,
  TargetPartRecordCount BIGINT,
  SourcePartFileCount BIGINT,
  TargetPartFileCount BIGINT,
  SourcePartSize BIGINT,
  TargetPartSize BIGINT,
  SourcePath STRING,
  TargetPath STRING,
  Status STRING
  ) Partitioned BY (partition_date STRING );