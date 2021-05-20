# Prerequisites 
- Snowflake Environment
- TigerGraph 3.1.1+ Instance 

## Program Description
In the current folder, you will be able to find the two files listed below.
These files are for automatically generating and formattingthe connector.yaml file, which is expected by TigerGraphConnector.jar.
```
SF2TGconfig.jar 
SF2TG.sh
```

## Application Usage
```
./SF2TG.sh [-h] [[sfuser=<SFUsername> sfpassword=<SFPassword> sfurl=<SFURL> 
                 sfdb=<SFDatabase> sfschema=<SFSchema> tguser=<TGUsername> tgpassword=<TGPassword> 
                 tgip=<TGMachineIP> tgtoken=<TGToken> graph=<TGGraphName> 
                 <SFTablename1:TGLoadingJob1 SFTablename2:TGLoadingJob2 ...>]
```

## Examples
### -h flag
./SF2TG.sh -h will simply print out the usage of the script:
```
Usage: ./SF2TG [-h] [sfuser=<SFUsername> sfpassword=<SFPassword> sfurl=<SFURL> sfdb=<SFDatabase> sfschema=<SFSchema> tguser=<TGUsername> tgpassword=<TGPassword> tgip=<TGMachineIP> tgtoken=<TGToken> graph=<TGGraphName> <SFTablename1:TGLoadingJob1 SFTablename2:TGLoadingJob2 ...>]

```

### Normal usage
Here is an example of calling the script to get actual output:
```
./SF2TG.sh sfuser=kcai sfpassword=Wasdqer1 sfurl=fka25931.us-east-1 sfdb=TEST_CONNECT sfschema=Friend tguser=tigergraph tgpassword=tigergraph tgip=3.84.42.120 graph=SimSwapPoC tgtoken=do5t6bmng5kaumbaufrbsghr0dr9cigs COWORKERS:load_job_IMSI_csv_1619483721639 PERSONS:load_incident FRIENDS:fakeLoader

Connecting to Snowflake
Snowflake Connected

Getting tables
Getting columns for table: COWORKERS
Getting columns for table: FRIENDS
Getting columns for table: PERSONS

Snowflake tables: [COWORKERS, FRIENDS, PERSONS]
Snowflake columns: {COWORKERS=[PERSON1, PERSON2], FRIENDS=[PERSON1, PERSON2], PERSONS=[ID]}

TG loading jobs: {load_job_keywords_csv_1619483014492=[MyDataSource], load_incident=[f], load_job_IMSI_csv_1619483721639=[MyDataSource], load_job_calldata_csv_1619484409697=[MyDataSource], load_job_memo_txt_1619482873692=[MyDataSource], fakeLoader=[f1, f2], whereLoading=[f1], edgeLoad=[f], load_job_subscriber_csv_1619483264132=[MyDataSource]}

Printing to YAML file: /Users/kevin/Desktop/connector.yaml
```

## Errors
If certain fields are missing, and are required to access either SnowFlake or TigerGraph, the user will be prompted to double check their input.
```
Missing essential information. Check inputs
```

SnowFlake user credentials are incorrect:
```
Unable to connect to Snowflake. Please check your provided credentials"
```

Provided database or schema doesn't exist in SnowFlake:
```
Invalid database or schema, please verify your Snowflake DB.
```

Provided TG Token is invalid:
```
Invalid Token for TG Graph: abc123456123. Exiting...
```

Provided SnowFLake Table to TigerGraph Job mapping is incorrect:
```
Unable to find TigerGraph loading jobs. Please check your TigerGraph Instance.
```
