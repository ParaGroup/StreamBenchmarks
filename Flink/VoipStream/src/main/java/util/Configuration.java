package util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class Configuration {
    private JsonNode configuration;

    public Configuration(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        configuration = mapper.readTree(new File(path));
    }

    public static Configuration fromArgs(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Parameters: <configuration_json_file>");
            System.exit(1);
        }

        String path = args[0];
        return new Configuration(path);
    }

    public JsonNode getTree() {
        return configuration;
    }
}
