import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;

import java.io.*;
import java.util.*;

public class YamlCreation {
    public static void writeToYaml(ArrayList<String> tables, HashMap<String, String> jobMap, HashMap<String,List<String>> tableMap, HashMap<String, Set<String>> tgMap, connectConfigs config) throws FileNotFoundException {
        ArrayList<String> mappingTables = new ArrayList<>();
        HashMap<String, String> sfConfigMap = new HashMap<>();
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


        // write sf configs to file
        sfConfigMap.put("sfURL", config.getSfURL() + ".snowflakecomputing.com");
        sfConfigMap.put("sfUser", config.getSfUser());
        sfConfigMap.put("sfPassword", config.getSfPassword());
        sfConfigMap.put("sfDatabase", config.getSfDatabase());
        sfConfigMap.put("sfSchema",config.getSfSchema());
        prettyYaml.dump(sfConfigMap, writer);
        writer.println();

        for (String s : tables) {
            if (jobMap.containsKey(s)) {
                mappingTables.add(s);
            }
        }

        // write table names to file
        writer.println("sfDbtable: " + mappingTables);
        writer.println();

        // write tg configs to file
        tgConfigMap.put("driver", config.getDriver());
        tgConfigMap.put("url", config.getUrl());
        tgConfigMap.put("username", config.getUsername());
        tgConfigMap.put("password", config.getPassword());
        tgConfigMap.put("graph", config.getGraph());
        if (config.getToken() != null) {
            tgConfigMap.put("token", config.getToken());
        }
        prettyYaml.dump(tgConfigMap, writer);
        writer.println();

        // write job configs to file
        jobConfig.put("batchsize", config.getBatchSize());
        jobConfig.put("sep", config.getSep());
        jobConfig.put("debug", config.getDebug());
        jobConfig.put("numPartitions", config.getNumPartitions());
        prettyYaml.dump(jobConfig, writer);
        writer.println("eol: \"" + config.getEol() +"\"");
        writer.println();

        // write mappings to file
        writer.println("mappingRules:");

        for (String tableName : tableMap.keySet()) {
            if (jobMap.get(tableName) != null) {
                String[] jobAndFile = jobMap.get(tableName).split(":", 2);
                if (tgMap.get(jobAndFile[0]) != null) {
                        writer.println("  " + tableName + ":");
                        writer.println("    \"dbtable\": " + "\"" + "job " + jobAndFile[0] + "\"");
                        writer.println("    \"jobConfig\":");
                        writer.print("      \"sfColumn\": \"" + tableMap.get(tableName).get(0));
                        for (int i = 1; i < tableMap.get(tableName).size(); i++) {
                            writer.print("," + tableMap.get(tableName).get(i));
                        }
                        writer.println("\"");
                        writer.println("      \"filename\": " + jobAndFile[1]);
                }
            }
        }
        writer.flush();
        writer.close();

    }

}
