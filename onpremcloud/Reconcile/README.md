# DataCopy OnPrem to Cloud Deployment Process
#### create Hive table
```
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
```

#### deploy Nifi flow
https://github.com/harigmatta/Executables/blob/main/DataCopy_Recon_Changes.xml

#### deploy shell script
```
onprem_to_cloud_recon_cdp.sh

# sample
sh onprem_to_cloud_recon_cdp_local.sh "1.keytab" "alf@VPC.com" "system1" "singpore" "hdfs://pg1.vpc.cloudera.com:8020/warehouse/tablespace/external/hive" \
"abfs://datafs@hmattacdp.dfs.core.windows.net/data/sdm/srcsystem/country/hdata/" "t3" "2022-03-01" "bank1.db" "sri_open:ods,sri_nonopen:nds,storage:vds,bank1.db:log_time,db2:log_time" "recon" "INC_ONPREM_CLOUD_RECON"
```

#### deploy spark job