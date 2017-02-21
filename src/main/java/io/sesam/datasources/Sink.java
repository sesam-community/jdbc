package io.sesam.datasources;

import com.google.gson.stream.JsonReader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public interface Sink {
    void configure(Connection conn) throws SQLException;

    void readEntities(JsonReader jr, Connection conn, boolean isFull) throws SQLException, IOException;
}
