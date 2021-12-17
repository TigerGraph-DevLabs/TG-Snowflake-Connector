

# TG-Snowflake-Connector Test Case

### Test environment

- Java 8+
- Maven 3.5.4+

### Download TG-Snowflake-Connector Source

```shell
$ git clone git@github.com:TigerGraph-DevLabs/TG-Snowflake-Connector.git
$ cd TG-Snowflake-Connector/
# compile and package
$ mvn clean package

# If you get error:
[INFO] ------------------------------------------------------------------------
[INFO] BUILD FAILURE
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  38.570 s
[INFO] Finished at: 2021-11-23T01:23:29Z
[INFO] ------------------------------------------------------------------------
[ERROR] Failed to execute goal on project TigerGraphConnector: Could not resolve dependencies for project com.tigergraph:TigerGraphConnector:jar:1.0-SNAPSHOT: Could not find artifact com.tigergraph:tg-java-driver:jar:1.2 in central (https://repo.maven.apache.org/maven2) -> [Help 1]

# You need install tg-driver:
$ mvn install:install-file -Dfile=/{location}/tg-java-driver-1.2.jar -DgroupId=com.tigergraph -DartifactId=tg-java-driver -Dversion=1.2 -Dpackaging=jar
$ mvn clean package
# package success, get connector package and config file:
$ cd target
# get TigerGraphConnector-1.0-SNAPSHOT.jar
$ cd classes
# get connector conf connector.yaml
$ vim connector.yaml
# Config your connector task;
# If we use private pem key to connect SnowFlake, follow this:
# Support unencrypted private key to connect snowflake. [Reference](https://docs.snowflake.com/en/user-guide/key-pair-auth.html#supported-snowflake-clients)
```

### Install Spark

​    Note that, Spark 2.x is pre-built with Scala 2.11 except version 2.4.2, which is pre-built with Scala 2.12. Spark 3.0+ is pre-built with Scala 2.12. 

​    Our source code build in Scala 2.11. 

```java
If unsupport spark version used, we will get exception:
java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps;
```

​    Install steps:

```shell
$ wget https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.6.tgz
$ tar -zxf spark-2.4.5-bin-hadoop2.6.tgz
# you can config spark home like this:
# vi /etc/profile
# export SPARK_HOME=/home/hadoop/spark-2.2.0-bin-hadoop2.6
# export PATH=$SPARK_HOME/bin:$PATH
$ cd $SPARK_HOME
# running command:
$ bin/spark-submit \
--class com.tigergraph.spark_connector.SF2Spark2Tg \
/{path}/TigerGraphConnector-1.0-SNAPSHOT.jar \
/{path}/connector.yaml
```

### Test steps:

1. Prepare SF account:
   - create database 
   - upload test data to sf 
   - create private pem key(https://docs.snowflake.com/en/user-guide/key-pair-auth.html#supported-snowflake-clients)
2. Prepare tigergraph db:
   - create vertex and graph
   - create loading job
3. Config yaml file:

```shell
# snowflake config
sfURL: lla10179.us-east-1.snowflakecomputing.com
sfUser: liusx
#sfPassword: Qwer1234
sfDatabase: tg_spark
sfSchema: synthea30g
warehouse: COMPUTE_WH
#application:
pem_private_key: /home/ubuntu/sfKey/rsa_key.p8
# SnowFlake tables  -> table to import
sfDbtable: [conditions,providers]

# tigergraph config
driver: com.tigergraph.jdbc.Driver
url: jdbc:tg:http://127.0.0.1:14240
username: tigergraph
password: tigergraph
# token: k670rl16oncs359la8tf2upb5jlmrfg0
# graph name
graph: test

batchsize: 5000
# This symbol is used to represent a delimiter during transmission. No symbol can exist in the data
sep: ','
# This symbol is used to represent a newline during transmission. No symbol can exist in the data
eol: "\n"
debug: 0
# Maximum number of partitions for Spark
numPartitions: 150

# Data structure mapping
# key : SnowFlake table
# value : tg loading job mapping
#     dbtable : loading job name
#     jobConfig: config loading job
#         sfColumn : columns in SnowFlake table
#         filename: filename defined in the loading job
mappingRules:
  conditions:
    "dbtable": "job loadJob"
    "jobConfig":
      "sfColumn": "PATIENT,ENCOUNTER,CODE,DESCRIPTION"
      # filename defined in the loading job
      "filename": user
  providers:
    "dbtable": "job loadJob"
    "jobConfig":
      "sfColumn": "ID,ORGANIZATION,NAME"
      "filename": test
```

4. Query vertex num in graph:

```shell
GSQL > BEGIN
GSQL > INTERPRET QUERY () FOR GRAPH test {
GSQL >   # declaration statements
GSQL >   test = {Test.*};
GSQL >   # body statements
GSQL >   PRINT test.size();
GSQL > } 
GSQL > END
{
  "error": false,
  "message": "",
  "version": {
    "schema": 0,
    "edition": "enterprise",
    "api": "v2"
  },
  "results": [{"test.size()": 0}]
}

GSQL > BEGIN
GSQL > INTERPRET QUERY () FOR GRAPH test {
GSQL >   # declaration statements
GSQL >   users = {User.*};
GSQL >   # body statements
GSQL >   PRINT users.size();
GSQL > } 
GSQL > END
{
  "error": false,
  "message": "",
  "version": {
    "schema": 0,
    "edition": "enterprise",
    "api": "v2"
  },
  "results": [{"users.size()": 0}]
}
```

5. Submit task to spark

   ```shell
   $ bin/spark-submit \
   --class com.tigergraph.spark_connector.SF2Spark2Tg \
   /{path}/TigerGraphConnector-1.0-SNAPSHOT.jar \
   /{path}/connector.yaml
   ```

   running log:

```shell
21/11/23 03:22:24 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, ip-172-31-31-112, 39159, None)
21/11/23 03:22:54 INFO loadBegin: [ conditions ] begin load data
21/11/23 03:22:56 INFO loadBegin: [ providers ] begin load data
21/11/23 03:23:01 INFO progress: Write Data Progress:                                                    |0%
21/11/23 03:23:06 INFO progress: Write Data Progress:                                                    |0%
21/11/23 03:23:11 INFO progress: Write Data Progress:                                                    |0%
21/11/23 03:23:16 INFO progress: Write Data Progress:                                                    |0%
21/11/23 03:23:21 INFO progress: Write Data Progress:                                                    |0%
21/11/23 03:23:26 INFO progress: Write Data Progress:                                                    |0%
21/11/23 03:23:31 INFO progress: Write Data Progress:                                                    |0%
21/11/23 03:23:36 INFO progress: Write Data Progress:                                                    |0%
21/11/23 03:23:41 INFO loadSuccess: [ providers ] load success, consume time: 44 s
21/11/23 03:23:42 INFO progress: Write Data Progress: *************************                          |50%
21/11/23 03:23:47 INFO progress: Write Data Progress: *************************                          |50%
21/11/23 03:23:52 INFO progress: Write Data Progress: *************************                          |50%
21/11/23 03:23:57 INFO progress: Write Data Progress: *************************                          |50%
21/11/23 03:24:02 INFO progress: Write Data Progress: *************************                          |50%
21/11/23 03:24:07 INFO progress: Write Data Progress: *************************                          |50%
21/11/23 03:24:12 INFO progress: Write Data Progress: *************************                          |50%
21/11/23 03:24:17 INFO loadSuccess: [ conditions ] load success, consume time: 82 s
21/11/23 03:24:17 INFO progress: The total time consuming:120s
```

6. Query vertex num in graph:

   ```shell
   GSQL > BEGIN
   GSQL > INTERPRET QUERY () FOR GRAPH test {
   GSQL >   # declaration statements
   GSQL >   test = {Test.*};
   GSQL >   # body statements
   GSQL >   PRINT test.size();
   GSQL > } 
   GSQL > END
   {
     "error": false,
     "message": "",
     "version": {
       "schema": 0,
       "edition": "enterprise",
       "api": "v2"
     },
     "results": [{"test.size()": 9815}]
   }
   
   GSQL > BEGIN
   GSQL > INTERPRET QUERY () FOR GRAPH test {
   GSQL >   # declaration statements
   GSQL >   users = {User.*};
   GSQL >   # body statements
   GSQL >   PRINT users.size();
   GSQL > } 
   GSQL > END
   {
     "error": false,
     "message": "",
     "version": {
       "schema": 0,
       "edition": "enterprise",
       "api": "v2"
     },
     "results": [{"users.size()": 4226}]
   }
   ```

### Enter SnowFlake password manually Test
Submit task to spark. Then enter the password according to the situation.
```shell script
Please enter the snowflake password, end by 'Enter'. If you don’t need to enter a password, just hit enter.
Enter SnowFlake password : 
```

   