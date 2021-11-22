## 1. Prerequisite

**Before you begin, make sure you have the following environment**

- Spark 2.4.6+ environment available, standalone or other mode set-up
- Snowflake environment
- TigerGraph 3.1.1+

### 1.1. Folder description

You can find the jar and configuration file in the current folder.

```shell
$ ls
TigerGraphConnector.jar  connector.yaml
$ pwd
/home/ubuntu/tiger_connector
```

## 2. Configuration

**Before execute the task, you need to modify the configuration file(connector.yaml) as follows**

Once the configuration file is written, it can be placed anywhere.

### Explain

1. TIGER_IP stands for TigerGraph's IP
2. SPARK_IP stands for Spark Master's IP

### 2.1 SnowFlake connection related information

```yaml
# snowflake config
sfURL: lla10179.us-east-1.snowflakecomputing.com
sfUser: liusx
sfPassword: WxW7b6xJtW
sfDatabase: tg_spark
sfSchema: synthea30g
```
```yaml
pem_private_key: 
```
Support unencrypted private key to connect snowflake. [Reference](https://docs.snowflake.com/en/user-guide/key-pair-auth.html#supported-snowflake-clients)

### 2.2 SnowFlake table name to be imported

```yaml
sfDbtable: [patients,conditions,devices,encounters,observations]
```

### 2.3 TigerGraph related information

```yaml
# tigergraph config
driver: com.tigergraph.jdbc.Driver
# Replace TIGER_IP with tigergraph's ip
url: jdbc:tg:http://TIGER_IP:14240
username: tigergraph
password: tiger
token: k670rl16ons3536a8tf2upb5jlmrfg0
# graph name
graph: synthea
```

**Note:** If authentication is turned on, the user name and password verification is invalid, and token authentication must be used

If the token is configured, username and password can be configured without

### 2.4 Transmission configuration

```yaml
batchsize: 5000
# This symbol is used to represent a delimiter during transmission.
sep: ','
# This symbol is used to represent a newline during transmission.
eol: "\n"
debug: 0
# Maximum number of partitions for Spark.If the data volume is too large, consider increasing this value
numPartitions: 150
```

### 2.5 Loading job name and SnowFlake corresponding column

```yaml
mappingRules:
  # Table name in snowFlake
  conditions:
    # loading job name
    "dbtable": "job loadCondition"
    "jobConfig":
      # The order of the columns that need to be selected in snowFlake must be the same as the order in the loading job, otherwise there will be problems inserting data
      "sfColumn": "START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION"
      #Filename defined in the loading job.It must be the same as the FILENAME in the loading job, otherwise the import will be problematic
      "filename": condition_file
  providers:
    "dbtable": "job loadProvider"
    "jobConfig":
      "sfColumn": "ID,ORGANIZATION,NAME,GENDER,SPECIALITY,ADDRESS,CITY,STATE,ZIP,LAT,LON,UTILIZATION"
      "filename": provider_file
```

### 2.6 tigergraph create loading job

You must install the following Loading Job in the TigerGraph shell.

```scala
CREATE LOADING JOB loadCondition FOR GRAPH synthea {
  DEFINE FILENAME condition_file;
  LOAD condition_file TO VERTEX Condition VALUES($4, $4, $5, $0, $1) USING SEPARATOR=",", HEADER="true", EOL="\n"; 
  LOAD condition_file TO EDGE hasCondition VALUES($2, $4, $0, $1) USING SEPARATOR=",", HEADER="true", EOL="\n";
}


CREATE LOADING JOB loadProvider FOR GRAPH synthea {
  DEFINE FILENAME provider_file; 

  LOAD provider_file TO VERTEX Provider VALUES($0, $2, $3, $4, $11) USING SEPARATOR=",", HEADER="true", EOL="\n"; 
  LOAD provider_file TO VERTEX Address VALUES($0, $5) USING SEPARATOR=",", HEADER="true", EOL="\n"; 
  LOAD provider_file TO EDGE worksAt VALUES($0, $1) USING SEPARATOR=",", HEADER="true", EOL="\n"; 
  LOAD provider_file TO EDGE addressInZip VALUES($5, $8) USING SEPARATOR=",", HEADER="true", EOL="\n"; 
  LOAD provider_file TO EDGE zipInCity VALUES($8, $6) USING SEPARATOR=",", HEADER="true", EOL="\n"; 
  LOAD provider_file TO EDGE zipInState VALUES($8, $7) USING SEPARATOR=",", HEADER="true", EOL="\n"; 
  LOAD provider_file TO EDGE cityInState VALUES($7, $7) USING SEPARATOR=",", HEADER="true", EOL="\n";
}
```

### Example code location description

```
# The location of the configuration file
/home/ubuntu/tiger_connector/connector.yaml

# The location of the JAR package
/home/ubuntu/tiger_connector/TigerGraphConnector.jar
```



## 3.Execute the task

```shell
# Enter spark directory
$ cd $SPARK_HOME

# Need to modify the spark master address
# The test uses standalone build
# If you use mesos or other spark clusters, you need to modify the startup command
$ bin/spark-submit --class com.tigergraph.spark_connector.SF2Spark2Tg --master spark://SPARK_IP:7077 /home/ubuntu/tiger_connector/TigerGraphConnector.jar /home/ubuntu/tiger_connector/connector.yaml
```

**Before execute the task, you can query table record number or truncate table**

### 3.1 Query record number

If the authentication is turned on, you need to add a token when you use this method to clear the graph and query the graph.

```shell
curl -X DELETE -H "Authorization: Bearer k670rl16oncs359la8tf2upb5jlmrfg0" "http://TIGER_IP:9000/graph/synthea/vertices/Patient?count_only=true"
```

```shell
# You need to install jq first
sudo apt install jq

# Number of vertex
curl -X POST 'http://TIGER_IP:9000/builtins/synthea' -d  '{"function":"stat_vertex_number","type":"*"}' | jq .

# Number of edge
curl -X POST 'http://TIGER_IP:9000/builtins/synthea' -d  '{"function":"stat_edge_number","type":"*"}' | jq .
```

## 4. Spark task web

```
# spark cluster view
http://SPARK_IP:8080/

#  current task performance view
http://SPARK_IP:4040/
```

## 5. Query the number of records written to tg

```shell
# Number of vertex
curl -X POST 'http://TIGER_IP:9000/builtins/synthea' -d  '{"function":"stat_vertex_number","type":"*"}' | jq .

# Number of edge
curl -X POST 'http://TIGER_IP:9000/builtins/synthea' -d  '{"function":"stat_edge_number","type":"*"}' | jq .
```

## 6. Console log instructions

```
# in write method: When the write method is entered it is printed
21/03/11 10:31:29 INFO loadBegin: [ patients ] begin load data
21/03/11 10:31:30 INFO loadBegin: [ conditions ] begin load data

# in write method: Print when the tables write to tiger graph is complete
21/03/11 10:31:41 INFO loadSuccess: [ payers ] load success, consume time: 5 s
21/03/11 10:31:41 INFO loadBegin: [ organizations ] begin load data
21/03/11 10:31:42 INFO loadSuccess: [ devices ] load success, consume time: 10 s
21/03/11 10:31:45 INFO loadSuccess: [ patients ] load success, consume time: 16 s
21/03/11 10:31:45 INFO loadBegin: [ imaging_studies ] begin load data
21/03/11 10:31:47 INFO loadBegin: [ medications ] begin load data

# in main method: Emitted if a table has been written or has not been printed for 5 seconds
# Progress is calculated by the number of successes written to the table

# example: 
# total tables：[patients,conditions,devices,encounters,observations,payers,immunizations,allergies,procedures,providers]

# success write tables : [devices,encounters]

# progress : 2/10 = 0.2 = 20%
21/03/11 10:31:48 INFO progress: Write Data Progress: **********                                         |20%

# at the end of the main method: It waits until all the tables have been written
21/03/11 10:32:39 INFO progress: The total time consuming:80s
```

## Appendix

### Creating Tokens

[User Privileges and Authentication](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#creating-tokens)

A user must have a secret before they create a token. Secrets are generated in GSQL (see CREATE SECRET below). The endpoint [`GET /requesttoken`](https://docs.tigergraph.com/dev/restpp-api/intro#requesting-a-token-with-get-requesttoken) is used to create a token. The endpoint has two parameters:

- secret (required): the user's secret
- lifetime (optional): the lifetime for the token, in seconds. The default is one month, approximately 2.6 million seconds.

```shell
curl -X GET 'TIGER_IP:9000/requesttoken?secret=jiokmfqqfu2f95qs6ug85o89rpkneib3&lifetime=1000000'
```
