package io.debezium.connector.gaussdb.connection.gsoutput;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.regex.Matcher;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.gaussdb.PostgresStreamingChangeEventSource;
import io.debezium.connector.gaussdb.PostgresType;
import io.debezium.connector.gaussdb.TypeRegistry;
import io.debezium.connector.gaussdb.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.gaussdb.connection.ReplicationMessage;
import io.debezium.connector.gaussdb.connection.ReplicationMessageColumnValueResolver;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.Value;

/**
 * @author yuez
 * @since 2025/11/23
 */
public class GsOutputDdlReplicationMessage implements ReplicationMessage {

    private final Document rawMessage;
    private final TypeRegistry typeRegistry;
    private Instant commitTime;
    private Long transactionId;
    private List<Column> oldColumns;
    private List<Column> newColumns;

    public GsOutputDdlReplicationMessage(Document rawMessage, TypeRegistry typeRegistry, Instant commitTime, Long transactionId) {
        this.rawMessage = rawMessage;
        this.typeRegistry = typeRegistry;
        this.commitTime = commitTime;
        this.transactionId = transactionId;
        this.newColumns = transform(rawMessage, "columns_name", "columns_type", "columns_val");
        this.oldColumns = transform(rawMessage, "old_keys_name", "old_keys_type", "old_keys_val");
    }

    @Override
    public Operation getOperation() {
        final String operation = rawMessage.getString("op_type");
        switch (operation) {
            case "INSERT":
                return Operation.INSERT;
            case "UPDATE":
                return Operation.UPDATE;
            case "DELETE":
                return Operation.DELETE;
        }
        throw new IllegalArgumentException(
                "Unknown operation '" + operation + "' in replication stream message");
    }

    @Override
    public Instant getCommitTime() {
        return this.commitTime;
    }

    @Override
    public OptionalLong getTransactionId() {
        return this.transactionId == null ? OptionalLong.empty() : OptionalLong.of(transactionId);
    }

    @Override
    public String getTable() {
        return rawMessage.getString("table_name");
    }

    @Override
    public List<Column> getOldTupleList() {
        return this.oldColumns;
    }

    @Override
    public List<Column> getNewTupleList() {
        return this.newColumns;
    }

    @Override
    public boolean hasTypeMetadata() {
        return true;
    }

    @Override
    public boolean isLastEventForLsn() {
        return true;
    }

    private List<Column> transform(final Document data, final String nameField, final String typeField, final String valueField) {
        final Array columnNames = data.getArray(nameField);
        final Array columnTypes = data.getArray(typeField);
        final Array columnValues = data.getArray(valueField);

        if (columnNames.size() != columnTypes.size() || columnNames.size() != columnValues.size()) {
            throw new ConnectException("Column related arrays do not have the same size");
        }

        final List<Column> columns = new ArrayList<>(columnNames.size());

        for (int i = 0; i < columnNames.size(); i++) {
            final String columnName = columnNames.get(i).asString();
            final String columnTypeName = columnTypes.get(i).asString();
            final Value rawValue = columnValues.get(i);
            final PostgresType columnType = typeRegistry.get(parseType(columnName, columnTypeName));
            columns.add(new AbstractReplicationMessageColumn(columnName, columnType, columnTypeName, true, true) {

                @Override
                public Object getValue(PostgresStreamingChangeEventSource.PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
                    return GsOutputDdlReplicationMessage.this.getValue(columnName, columnType, columnTypeName, rawValue, connection, includeUnknownDatatypes,
                            typeRegistry);
                }

                @Override
                public String toString() {
                    return columnName + "(" + columnTypeName + ")=" + rawValue;
                }
            });
        }

        return columns;
    }

    private String parseType(String columnName, String typeWithModifiers) {
        Matcher m = AbstractReplicationMessageColumn.TypeMetadataImpl.TYPE_PATTERN.matcher(typeWithModifiers);
        if (!m.matches()) {
            throw new ConnectException(String.format("Failed to parse columnType '%s' for column %s", typeWithModifiers, columnName));
        }
        String baseType = m.group("base").trim();
        final String suffix = m.group("suffix");
        if (suffix != null) {
            baseType += suffix;
        }
        baseType = TypeRegistry.normalizeTypeName(baseType);
        if (m.group("array") != null) {
            baseType = "_" + baseType;
        }
        return baseType;
    }

    public static Object getValue(String columnName, PostgresType type, String fullType, Value rawValue,
                                  final PostgresStreamingChangeEventSource.PgConnectionSupplier connection,
                                  boolean includeUnknownDataTypes, TypeRegistry typeRegistry) {
        final GsOutputColumnValue columnValue = new GsOutputColumnValue(rawValue);
        return ReplicationMessageColumnValueResolver.resolveValue(columnName, type, fullType, columnValue, connection, includeUnknownDataTypes, typeRegistry);
    }

}
