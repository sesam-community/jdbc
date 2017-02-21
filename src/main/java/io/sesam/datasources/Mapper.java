package io.sesam.datasources;

import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Mapper implements AutoCloseable {

    static Logger log = LoggerFactory.getLogger(Mapper.class);
    
    private Map<String,DataSystem> systems = new ConcurrentHashMap<>();

    public Mapper(Map<String,DataSystem> systems) {
        this.systems = systems;            
    }

    @Override
    public void close() {
        for (DataSystem system : this.systems.values()) {
            try {
                system.close();
            } catch (Exception e) {
                log.error("Got exception", e);
            }
        }
    }

    public boolean isValidSource(String systemId, String sourceId) {
        DataSystem system = this.systems.get(systemId);
        return system != null && system.isValidSource(sourceId);
    }

    public boolean isValidSink(String systemId, String sinkId) {
        DataSystem system = this.systems.get(systemId);
        return system != null && system.isValidSink(sinkId);
    }

    public void writeEntities(JsonWriter jw, String systemId, String sourceId, String since) throws SQLException, IOException {
        DataSystem system = this.systems.get(systemId);
        assert system != null;
        system.writeEntities(jw, sourceId, since);
    }


    public void readEntities(JsonReader jr, String systemId, String sinkId, boolean isFull) throws SQLException, IOException {
        DataSystem system = this.systems.get(systemId);
        assert system != null;
        system.readEntities(jr, sinkId, isFull);
    }
    
    public static Mapper load(String filename) throws Exception {
        try (FileReader reader = new FileReader(filename)) {
            Gson gson = new Gson();
            JsonObject root = gson.fromJson(reader, JsonObject.class);
            
            Map<String,DataSystem> systems = new HashMap<>();
            for (Entry<String, JsonElement> e : root.entrySet()) {
                String systemId = e.getKey();
                DataSystem system = newSystem(systemId, e.getValue());
                system.configure();
                systems.put(systemId, system);
            }
            return new Mapper(systems);
        }
    }

    private static DataSystem newSystem(String systemId, JsonElement systemElem) {
        if (!systemElem.isJsonObject()) {
            throw new RuntimeException("Invalid configuration for system '" + systemId + "': " + systemElem); 
        }
        
        // jdbc data source
        JsonObject systemObj = systemElem.getAsJsonObject();
        String jdbcUrl = getStringValue(systemObj, "jdbc-url");
        String username = getStringValue(systemObj, "username", null);
        String password = getStringValue(systemObj, "password", null);

        HikariConfig config = new HikariConfig();
        config.setInitializationFailFast(false);
        config.setConnectionTimeout(5000);
        config.setJdbcUrl(jdbcUrl);
        if (username != null) {
            config.setUsername(username);
        }
        if (password != null) {
            config.setPassword(password);
        }
        HikariDataSource ds = new HikariDataSource(config);
        
        // sources: tables and queries
        Map<String,Source> sources = new HashMap<>();
        if (systemObj.has("sources")) {
            JsonObject sourcesObj = systemObj.getAsJsonObject("sources");
            for (Entry<String, JsonElement> e : sourcesObj.entrySet()) {
                String sourceId = e.getKey();
                JsonObject sourceObj = e.getValue().getAsJsonObject();
                JsonElement pkElem = sourceObj.get("primary-key");
                List<String> primaryKeys = new ArrayList<>();
                if (pkElem.isJsonArray()) {
                    for (JsonElement pke : pkElem.getAsJsonArray()) {
                        primaryKeys.add(pke.getAsString());
                    }
                } else {
                    primaryKeys.add(pkElem.getAsString());
                }
                String updatedColumn = getStringValue(sourceObj, "updated-column", null);
                if (sourceObj.has("query")) {
                    String query = getStringValue(sourceObj, "query");
                    String since = getStringValue(sourceObj, "since", null);
                    sources.put(sourceId, new Query(query, since, primaryKeys, updatedColumn));
                } else {
                    sources.put(sourceId, new Table(sourceId, primaryKeys, updatedColumn));
                }
            }
        }

        // sinks
        Map<String,Sink> sinks = new HashMap<>();
        if (systemObj.has("sinks")) {
            JsonObject sourcesObj = systemObj.getAsJsonObject("sinks");
            for (Entry<String, JsonElement> e : sourcesObj.entrySet()) {
                String sinkId = e.getKey();
                JsonObject sinkObj = e.getValue().getAsJsonObject();
                String table = getStringValue(sinkObj, "table");
                JsonElement pkElem = sinkObj.get("primary-key");
                List<String> primaryKeys = new ArrayList<>();
                if (pkElem != null) {
                    if (pkElem.isJsonArray()) {
                        for (JsonElement pke : pkElem.getAsJsonArray()) {
                            primaryKeys.add(pke.getAsString());
                        }
                    } else {
                        primaryKeys.add(pkElem.getAsString());
                    }
                }
                List<String> whitelist = getStringValues(sinkObj, "whitelist");
                List<String> blacklist = getStringValues(sinkObj, "blacklist");
                String timestamp = getStringValue(sinkObj, "timestamp", "sesam-timestamp");
                boolean truncateOnFirstRun = getBooleanValue(sinkObj, "truncate_table_on_first_run", false);

                sinks.put(sinkId, new MatcherSink(sinkId, table, primaryKeys, truncateOnFirstRun, whitelist, blacklist, timestamp));
            }
        }
        return new DataSystem(ds, sources, sinks);
    }

    private static String getStringValue(JsonObject jo, String key) {
        if (jo.has(key)) {
            return jo.getAsJsonPrimitive(key).getAsString();
        } else {
            throw new RuntimeException("Missing '" + key + "' property in " + jo);
        }
    }

    private static String getStringValue(JsonObject jo, String key, String defaultValue) {
        if (jo.has(key)) {
            return jo.getAsJsonPrimitive(key).getAsString();
        } else {
            return defaultValue;
        }
    }

    private static List<String> getStringValues(JsonObject jo, String key) {
        List<String> values = new ArrayList<>();
        if (jo.has(key)) {
            for (JsonElement pke : jo.getAsJsonArray(key)) {
                values.add(pke.getAsString());
            }
        }
        return values;
    }

    private static boolean getBooleanValue(JsonObject jo, String key, boolean defaultValue) {
        if (jo.has(key)) {
            return jo.getAsJsonPrimitive(key).getAsBoolean();
        } else {
            return defaultValue;
        }
    }
}
