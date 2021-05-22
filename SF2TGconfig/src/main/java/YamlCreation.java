import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;

import java.io.*;
import java.util.*;

public class YamlCreation {
    public static void writeToYaml(ArrayList<String> tables, HashMap<String,String> jobMap, HashMap<String,List<String>> tableMap, HashMap<String, Set<String>> tgMap, connectConfigs config) throws FileNotFoundException {
        HashMap<String, String> sfConfigMap = new HashMap<>();
        HashMap<String, ArrayList<String>> dumpTables = new HashMap<>();
        HashMap<String, Object> jobConfig = new HashMap<>();
        HashMap<String, String> tgConfigMap = new HashMap<>();

        PrintWriter writer = new PrintWriter("./connector.yaml");

        SkipEmptyAndNullRepresenter skipEmptyAndNullRepresenter = new SkipEmptyAndNullRepresenter();
        skipEmptyAndNullRepresenter.addClassTag(connectConfigs.class, Tag.MAP);
        skipEmptyAndNullRepresenter.addClassTag(Mappings.class, Tag.MAP);

        // pretty yaml print option
        DumperOptions pretty = new DumperOptions();
        pretty.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        pretty.setPrettyFlow(true);

        // list yaml print option
        DumperOptions list = new DumperOptions();
        list.setDefaultFlowStyle(DumperOptions.FlowStyle.AUTO);

        Yaml prettyYaml = new Yaml(pretty);
        Yaml listYaml = new Yaml(skipEmptyAndNullRepresenter,list);

        for (Map.Entry<String,String> t : jobMap.entrySet()) {                     // for each table -> tg job mapped
            for (Map.Entry<String, List<String>> s : tableMap.entrySet()) {         // for each table
                if (s.getKey().equals(t.getKey())) {
                    for (Map.Entry<String, Set<String>> tg : tgMap.entrySet()) {    // for each tg job -> {files}
                        if (tg.getKey().equals(t.getValue())) {                     // if tg job name matches user mapping
                            Set<String> files = tg.getValue();
                            for (String file : files) {
                                Mappings tempMap = new Mappings();
                                tempMap.setfileName(file);
                                tempMap.settgLoadJob(tg.getKey());
                                if (s.getKey().equals(t.getKey())) {
                                    tempMap.setSFTable(s.getKey());
                                    tempMap.setsfColumns(s.getValue());
                                }
                                if (tempMap.getfileName() != null &&
                                        tempMap.gettgLoadJob() != null &&
                                        tempMap.getSFTable() != null &&
                                        tempMap.getsfColumns() != null) {
                                    config.setmappingRules(tempMap);
                                }
                            }

                        }
                    }
                }
            }
        }

        // write sf configs to file
        sfConfigMap.put("sfURL", config.getSfURL() + ".snowflakecomputing.com");
        sfConfigMap.put("sfUser", config.getSfUser());
        sfConfigMap.put("sfPassword", config.getSfPassword());
        sfConfigMap.put("sfDatabase", config.getSfDatabase());
        sfConfigMap.put("sfSchema",config.getSfSchema());
        prettyYaml.dump(sfConfigMap, writer);

        // write table names to file
        dumpTables.put("sfDbTable",tables);
        listYaml.dump(dumpTables, writer);

        // write tg configs to file
        tgConfigMap.put("driver", config.getDriver());
        tgConfigMap.put("url", config.getUrl());
        tgConfigMap.put("username", config.getUsername());
        tgConfigMap.put("password", config.getPassword());
        tgConfigMap.put("token", config.getToken());
        tgConfigMap.put("graph", config.getGraph());
        prettyYaml.dump(tgConfigMap, writer);

        // write job configs to file
        jobConfig.put("batchSize", config.getBatchSize());
        jobConfig.put("sep", config.getSep());
        jobConfig.put("eol", config.getEol());
        jobConfig.put("debug", config.getDebug());
        jobConfig.put("numPartitions", config.getNumPartitions());
        prettyYaml.dump(jobConfig, writer);

        // write mappings to file
        writer.println("mappingRules:");

        for (String tableName : tableMap.keySet()) {
            for (String filename: tgMap.get(jobMap.get(tableName))) {
                writer.println(" - " + tableName + ":");
                writer.println("    \"dbtable\": " + "\"" + "job " + jobMap.get(tableName) + "\"");
                writer.println("    \"jobConfig\":");
                writer.print("      \"sfColumn\": \"" + tableMap.get(tableName).get(0));
                for (int i = 1; i < tableMap.get(tableName).size(); i++) {
                    writer.print("," + tableMap.get(tableName).get(i));
                }
                writer.println("\"");
                writer.println("      \"filename\": " + filename);
            }
        }
        writer.flush();
        writer.close();

    }

}
