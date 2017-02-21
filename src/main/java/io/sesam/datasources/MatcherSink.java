package io.sesam.datasources;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class MatcherSink implements Sink {
    final Logger log;
    private final String table;
    private final List<String> primaryKeys;
    private final boolean truncateOnFirstRun;
    private final List<String> whitelist;
    private final List<String> blacklist;
    private final String timestamp;
    private List<String> pkColumns;
    private boolean useTimestamp;

    public MatcherSink(String sinkName, String table, List<String> primaryKeys, boolean truncateOnFirstRun, List<String> whitelist, List<String> blacklist, String timestamp) {
        log = LoggerFactory.getLogger(sinkName);
        this.table = table;
        this.primaryKeys = primaryKeys;
        this.truncateOnFirstRun = truncateOnFirstRun;
        this.whitelist = whitelist;
        this.blacklist = blacklist;
        this.timestamp = timestamp;
    }

    public void configure(Connection conn) throws SQLException {
        // TODO handle case matching
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet tables = metaData.getTables(null, null, table, null);
        boolean tableFound = false;
        while (tables.next()) {
            String tableName = tables.getString(3);
            log.info("found table: " + tableName);
            if (table.equals(tableName)) {
                tableFound = true;
                break;
            }
        }
        if (!tableFound) {
            throw new RuntimeException("No such table found: " + table);
        }
        ResultSet columns = metaData.getColumns(null, null, table, null);
        List<String> columnNames = new ArrayList<>();
        while (columns.next()) {
            String columnName = columns.getString(4);
            log.info("found column: " + columnName);
            columnNames.add(columnName);
        }
        if (columnNames.contains(timestamp)) {
            log.info("found timestamp column: " + timestamp);
            this.useTimestamp = true;
        }
        if (!this.primaryKeys.isEmpty()) {
            for (String pk : this.primaryKeys) {
                if (!columnNames.contains(pk)) {
                    throw new RuntimeException("No such column found: " + pk);
                }
            }
            this.pkColumns = primaryKeys;
        } else {
            ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, table);
            List<String> pkColumns = new ArrayList<>();
            while (primaryKeys.next()) {
                String pkColumn = primaryKeys.getString(4);
                log.info("found pk column: " + pkColumn);
                pkColumns.add(pkColumn);
            }
            this.pkColumns = pkColumns;
        }
    }

    @Override
    public void readEntities(JsonReader jr, Connection conn, boolean isFull) throws SQLException, IOException {
        if (isFull && truncateOnFirstRun) {
            String deleteAll = "DELETE FROM " + table;
            PreparedStatement deleteAllStmt = conn.prepareStatement(deleteAll);
            log.info("Deleting all from table: " + table);
            deleteAllStmt.execute();
            conn.commit();
        }
        try {
            jr.beginArray();
            while (jr.hasNext()) {
                readEntity(jr, conn);
            }
            conn.commit();
            jr.endArray();
        } catch (SQLException e) {
            log.warn("Rolling back entire batch after database exception", e);
            conn.rollback();
            throw e;
        }
    }

    private void readEntity(JsonReader jr, Connection conn) throws SQLException, IOException {
        jr.beginObject();
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        boolean isDeleted = false;
        while (jr.hasNext()) {
            String name = jr.nextName();
            if ("_deleted".equals(name)) {
                isDeleted = jr.nextBoolean();
            } else {
                Object value = getValue(jr);
                if (value != IGNORE && isListed(name)) {
                    values.put(name, value);
                }
            }
        }
        if (isDeleted) {
            delete(values, conn);
        } else {
            updateOrInsert(values, conn);
        }
        jr.endObject();
    }

    private boolean isListed(String name) {
        if (pkColumns.contains(name)) {
            // ignore blacklisted primary key columns
            return true;
        } else if (whitelist.isEmpty()) {
            return !blacklist.contains(name);
        } else {
            return whitelist.contains(name) && !blacklist.contains(name);
        }
    }

    private void delete(LinkedHashMap<String, Object> values, Connection conn) throws SQLException {
        String delete = "DELETE FROM " + table;
        StringJoiner columns = new StringJoiner(",");
        StringJoiner placeholders = new StringJoiner(",");
        for (String pk : pkColumns) {
            columns.add(pk + " = ?");
        }
        delete += " WHERE " + columns.toString();
        log.info(delete);
        PreparedStatement deleteStmt = conn.prepareStatement(delete);
        int paramIndex = 1;
        for (String pk : pkColumns) {
            deleteStmt.setObject(paramIndex++, values.get(pk));
        }
        deleteStmt.execute();
    }

    private void updateOrInsert(LinkedHashMap<String, Object> values, Connection conn) throws SQLException {
        // "update x set a = ? where z = ?"
        String update = "UPDATE " + table + " SET ";
        StringJoiner setters = new StringJoiner(",");
        for (String column : values.keySet()) {
            setters.add(column + " = ?");
        }
        if (useTimestamp) {
            // TODO or detect dialect and use now() functions in db
            setters.add(timestamp + " = ?");
        }
        update += setters.toString();
        StringJoiner wheres = new StringJoiner(",");
        for (String pks : pkColumns) {
            wheres.add(pks + " = ?");
        }
        update += " WHERE " + wheres.toString();
        log.info(update);
        PreparedStatement updateStmt = conn.prepareStatement(update);
        int paramIndex = 1;
        for (Map.Entry<String, Object> value : values.entrySet()) {
            // TODO could us setDate, setInt, etc.
            updateStmt.setObject(paramIndex++, value.getValue());
        }
        if (useTimestamp) {
            updateStmt.setDate(paramIndex++, new Date(System.currentTimeMillis()));
        }
        for (String pk : pkColumns) {
            updateStmt.setObject(paramIndex++, values.get(pk));
        }
        updateStmt.execute();
        int updated = updateStmt.getUpdateCount();
        if (updated > 1) {
            throw new SQLException("Expected 1 row to be updated, not " + updated);
        } else if (updated == 0) {
            insert(values, conn);
        }
    }

    private void insert(LinkedHashMap<String, Object> values, Connection conn) throws SQLException {
        // "insert into x (a, z) values (?, ?)"
        String insert = "INSERT INTO " + table + " (";
        StringJoiner columns = new StringJoiner(",");
        StringJoiner placeholders = new StringJoiner(",");
        for (String column : values.keySet()) {
            columns.add(column);
            placeholders.add("?");
        }
        if (useTimestamp) {
            columns.add(timestamp);
            columns.add("?");
        }
        insert += columns.toString() + ") VALUES (" + placeholders.toString() + ")";
        log.info(insert);
        PreparedStatement insertStmt = conn.prepareStatement(insert);
        int paramIndex = 1;
        for (Map.Entry<String, Object> value : values.entrySet()) {
            // TODO could us setDate, setInt, etc.
            insertStmt.setObject(paramIndex++, value.getValue());
        }
        if (useTimestamp) {
            insertStmt.setDate(paramIndex++, new Date(System.currentTimeMillis()));
        }
        insertStmt.execute();
    }

    static final Object IGNORE = new Object();

    private Object getValue(JsonReader jr) throws IOException {
        JsonToken type = jr.peek();
        if (type == JsonToken.BOOLEAN) {
            return jr.nextBoolean();
        } else if (type == JsonToken.NUMBER) {
            // could optimize to use other datatypes if possible
            return new BigDecimal(jr.nextString());
        } else if (type == JsonToken.NULL) {
            return null;
        } else if (type == JsonToken.STRING) {
            String val = jr.nextString();
            if (val.startsWith("~t")) {
                return DateTimeFormatter.ISO_LOCAL_DATE.parse(val.substring(2));
            } else if (val.startsWith("~")) {
                // what should we do with other transit types? let's ignore for now...
                return IGNORE;
            } else {
                return val;
            }
        } else {
            throw new RuntimeException("Unknown token: " + type);
        }
    }
}
