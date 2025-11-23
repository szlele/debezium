package io.debezium.connector.gaussdb.connection.gsoutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.gaussdb.jdbc.replication.fluent.logical.ChainedLogicalStreamBuilder;

import io.debezium.connector.gaussdb.TypeRegistry;
import io.debezium.connector.gaussdb.connection.AbstractMessageDecoder;
import io.debezium.connector.gaussdb.connection.DateTimeFormat;
import io.debezium.connector.gaussdb.connection.MessageDecoderContext;
import io.debezium.connector.gaussdb.connection.PostgresConnection;
import io.debezium.connector.gaussdb.connection.ReplicationMessage;
import io.debezium.connector.gaussdb.connection.ReplicationStream;
import io.debezium.connector.gaussdb.connection.TransactionMessage;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;

/**
 * GaussDB 逻辑复制消息解析
 * @author yuez
 * @since 2025/11/23
 */
public class GsOutputMessageDecoder extends AbstractMessageDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(GsOutputMessageDecoder.class);
    private static final Instant GS_EPOCH = LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);

    private final MessageDecoderContext decoderContext;
    private final PostgresConnection connection;

    /**
     * Will be null for a non-transactional decoding message
     */
    private Long transactionId;
    private Instant commitTime;

    private Pattern commit_pattern = Pattern.compile("COMMIT (\\d+) \\(at ([\\d-]+ [\\d:.+]+)\\) CSN (\\d+)");

    public GsOutputMessageDecoder(MessageDecoderContext decoderContext, PostgresConnection connection) {
        this.decoderContext = decoderContext;
        this.connection = connection;
    }

    @Override
    protected void processNotEmptyMessage(ByteBuffer buffer, ReplicationStream.ReplicationMessageProcessor processor, TypeRegistry typeRegistry)
            throws SQLException, InterruptedException {
        if (!buffer.hasArray()) {
            throw new IllegalStateException("Invalid buffer received from GS server during streaming replication");
        }
        final byte[] source = buffer.array();
        final byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length);
        String message = new String(content, StandardCharsets.UTF_8);
        /**
         * BEGIN 15432
         * {"table_name":"public.table_with_pk","op_type":"INSERT","columns_name":["a","b","c"],"columns_type":["integer","character varying","timestamp without time zone"],"columns_val":["65","'Backup and Restore'","'2025-11-23 00:09:34.522689'"],"old_keys_name":[],"old_keys_type":[],"old_keys_val":[]}
         * {"table_name":"public.table_without_pk","op_type":"UPDATE","columns_name":["a","b","c"],"columns_type":["integer","numeric","text"],"columns_val":["32","1.00","'Bar'"],"old_keys_name":["a","b","c"],"old_keys_type":["integer","numeric","text"],"old_keys_val":["32","1.00","'Foo'"]}
         * {"table_name":"public.table_without_pk","op_type":"DELETE","columns_name":[],"columns_type":[],"columns_val":[],"old_keys_name":["a","b","c"],"old_keys_type":["integer","numeric","text"],"old_keys_val":["32","1.00","'Baz'"]}
         * COMMIT 15432 (at 2025-11-22 16:09:34.531976+00) CSN 2994
         */
        try {
            if (message.startsWith("BEGIN")) {
                this.transactionId = Long.parseLong(message.substring(6));
                processor.process(new TransactionMessage(ReplicationMessage.Operation.BEGIN, transactionId, GS_EPOCH));
            }
            else if (message.startsWith("COMMIT")) {
                Matcher m = commit_pattern.matcher(message);
                if (m.find()) {
                    this.transactionId = Long.parseLong(m.group(1));
                    this.commitTime = DateTimeFormat.get().systemTimestampToInstant(m.group(2));
                    processor.process(new TransactionMessage(ReplicationMessage.Operation.COMMIT, this.transactionId, this.commitTime));
                }
                else {
                    throw new IllegalArgumentException("commit message format error: " + message);
                }
            }
            else if (message.startsWith("{") && message.endsWith("}")) {
                final Document document = DocumentReader.defaultReader().read(message.replace("'", "").replace("\"null\"", "null"));
                processor.process(new GsOutputDdlReplicationMessage(document, typeRegistry, this.commitTime, this.transactionId));
            }
            else {
                throw new IllegalArgumentException("Unsupported message: " + message);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithMetadata(ChainedLogicalStreamBuilder builder, Function<Integer, Boolean> hasMinimumServerVersion) {
        return builder;
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithoutMetadata(ChainedLogicalStreamBuilder builder, Function<Integer, Boolean> hasMinimumServerVersion) {
        return builder;
    }
}
