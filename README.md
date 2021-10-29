# TigerGraph-Snowflake-Connector



## 1. Introduction

This project enables users to load data from Snowflake database into TigerGraph database easily. For more details, refer to the [**documentation page**](https://github.com/TigerGraph-DevLabs/TG-Snowflake-Connector/wiki/TigerGraph-Snowflake-Connector-Documentation)

**If you:**
* are a developer and would like to look at the source code, the code is located in `src` folder.
* just want to use the connector, you can find the artifacts in 'assets' folder or review `tests` folder.

> Note: You can use QuickStart to get started with the connector or visit the [**documentation page**](https://github.com/TigerGraph-DevLabs/TG-Snowflake-Connector/wiki/TigerGraph-Snowflake-Connector-Documentation)

## 2. QuickStart

[friendNet](./tests/README.md)

Detailed documentation location：[TigerGraphConnector_readme](./assets/TigerGraphConnector_readme.md)



## 3. Folder description

```shell
# Connector that can be used, Configuration file and connector usage documentation
assets/

# source code
src/

# Use cases
# including Snowflake table building statement
# TigerGraph construction sentences and Loading Job
tests/
```

## 4. Development environment

- Java 8+

- Maven 3.5.4+

- Install tg-java-driver jar to your local maven repository.

  1. Get tg-java-driver-1.2.jar

  - Compile the source code https://github.com/tigergraph/ecosys/tree/master/tools/etl/tg-jdbc-driver

  - Or get it from : [SF2TGconfig](https://github.com/TigerGraph-DevLabs/TG-Snowflake-Connector/tree/main/SF2TGconfig)

  2. Install  to maven repository

     ```bash
     mvn install:install-file -Dfile=/{location}/tg-java-driver-1.2.jar -DgroupId=com.tigergraph -DartifactId=tg-java-driver -Dversion=1.2 -Dpackaging=jar
     ```

## 5. Code structure

```
src.main
	|--resources
		|--connector.yaml(configuration file)
	|--scala
		|--com.tigergraph.spark_connector
			|--reader(data origin)
			|--support(Read the configured support class)
			|--utils
			|--writer
			|--SF2Spark2Tg(main class)
```

