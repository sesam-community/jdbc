package io.sesam.datasources;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.stream.JsonWriter;

import spark.Spark;

public class App {

    static Logger log = LoggerFactory.getLogger(App.class);

    // TODO load config from environment variable or get it posted
    public static void main(String[] args) throws Exception {
        String configurationFile = args[0];
        log.info("Loading configuration from: " + configurationFile);
        Mapper mapper = Mapper.load(configurationFile);
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Spark.stop();
                mapper.close();
            }   
        }); 

        Spark.get("/:system/:source", (req, res) -> {
            res.type("application/json; charset=utf-8");
            String systemId = req.params("system");
            String sourceId = req.params("source");
            String since = req.queryParams("since");
            
            if (!mapper.isValidSource(systemId, sourceId)) {
                Spark.halt(404, "Unknown system/source pair.\n");
            }
            try {
                Writer writer = new OutputStreamWriter(res.raw().getOutputStream(), "utf-8");
                JsonWriter jsonWriter = new JsonWriter(writer);
                mapper.writeEntities(jsonWriter, systemId, sourceId, since);
                jsonWriter.flush();
            } catch (Exception e) {
                log.error("Got exception", e);
                Spark.halt(500);
            }
            return "";
        });

        Spark.post("/:system/:sink", (req, res) -> {
            res.type("application/json; charset=utf-8");
            String systemId = req.params("system");
            String sinkId = req.params("sink");
            boolean isFull = Boolean.parseBoolean(req.queryParams("is_full"));

            if (!mapper.isValidSink(systemId, sinkId)) {
                Spark.halt(404, "Unknown system/sink pair.\n");
            }
            try {
                Reader reader = new InputStreamReader(req.raw().getInputStream(), "utf-8");
                JsonReader jsonReader = new JsonReader(reader);
                mapper.readEntities(jsonReader, systemId, sinkId, isFull);
            } catch (Exception e) {
                log.error("Got exception", e);
                Spark.halt(500);
            }
            return "";
        });
    }

}
