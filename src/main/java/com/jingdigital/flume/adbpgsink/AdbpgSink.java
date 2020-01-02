package com.jingdigital.flume.adbpgsink;

import com.alibaba.cloud.analyticdb.adb4pgclient.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.Types;
import java.util.*;

public class AdbpgSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(AdbpgSink.class);

    private static final String ENCODING_PROP = "encoding";
    private static final String DEFAULT_ENCODING = "utf-8";
    private static final String BATCH_COUNT = "batch_count";
    private static final String ADBPG_TBL_NAME = "adbpg_tbl_name";
    private static final String ADBPG_SCHEMA_NAME = "adbpg_schema_name";
    private static final String ADBPG_HOST = "adbpg_host";
    private static final String ADBPG_PORT = "adbpg_port";
    private static final String ADBPG_USER = "adbpg_user";
    private static final String ADBPG_PASSWORD = "adbpg_password";
    private static final String ADBPG_DB = "adbpg_db";
    private static final int MAX_BATCH_COUNT = 10000;

    private int batchCount;
    private Charset charset;
    private String adbpgTblName;
    private String adbpgSchemaName;
    private Adb4pgClient adb4pgClient;

    @Override
    public Status process() throws EventDeliveryException {
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();

        try {
            List<Event> eventList = new ArrayList<Event>();

            for (int eventCount = 0; eventCount <= batchCount; ++eventCount) {
                Event event = ch.take();

                if (event == null) {
                    break;
                }

                eventList.add(event);
            }

            if (!processEventList(eventList)) {
                throw new RuntimeException("Failed to process event.");
            }

            try {
                adb4pgClient.commit();
            } catch (Throwable e) {
                throw e;
            }

            txn.commit();
        } catch (Throwable e) {
            txn.rollback();

            String msg = "Failed to commit transaction. Transaction rolled back.";
            logger.error(msg, e);
            if (!(e instanceof Error) && !(e instanceof RuntimeException)) {
                logger.error(msg, e);
                throw new EventDeliveryException(msg, e);
            }

            throw new RuntimeException(e);
        } finally {
            txn.close();
        }

        return Status.READY;
    }

    private boolean processEventList(List<Event> events) throws Throwable {
        for (Event event : events) {
            if (!processEvent(event)) {
                return false;
            }
        }

        return true;
    }

    private boolean processEvent(Event event) {
        String rawEvent = new String(event.getBody(), charset);

        logger.info("Event body: " + rawEvent);

        JSONArray jsonArray = JSON.parseArray(rawEvent);

        if (!rawEvent.isEmpty()) {
            int eventNum = jsonArray.size();
            for (int i = 0; i < eventNum; ++i) {
                Row row = new Row();
                JSONObject eventRow = jsonArray.getJSONObject(i);
                List<ColumnInfo> columnInfoList = adb4pgClient.getColumnInfo(adbpgTblName, adbpgSchemaName);
                addRowData(eventRow, row, columnInfoList);
                adb4pgClient.addRow(row, adbpgTblName, adbpgSchemaName);
            }
        }

        return true;
    }

    private void addRowData(JSONObject eventRow, Row row, List<ColumnInfo> columnInfoList) {
        int columnIndex = 0;
        for (ColumnInfo columnInfo : columnInfoList) {
            String columnName = columnInfo.getName();
            if (eventRow.get(columnName) != null) {
                ColumnDataType columnType = columnInfo.getDataType();
                switch (columnType.sqlType) {
                    case Types.TINYINT:
                        row.setColumn(columnIndex, eventRow.getByte(columnName));
                        break;
                    case Types.SMALLINT:
                        row.setColumn(columnIndex, eventRow.getShort(columnName));
                        break;
                    case Types.INTEGER:
                        row.setColumn(columnIndex, eventRow.getInteger(columnName));
                        break;
                    case Types.BIGINT:
                        row.setColumn(columnIndex, eventRow.getBigInteger(columnName));
                        break;
                    case Types.FLOAT:
                    case Types.REAL:
                        row.setColumn(columnIndex, eventRow.getFloat(columnName));
                        break;
                    case Types.DOUBLE:
                        row.setColumn(columnIndex, eventRow.getDouble(columnName));
                        break;
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        row.setColumn(columnIndex, eventRow.getBigDecimal(columnName));
                        break;
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIMESTAMP:
                    case Types.TIME_WITH_TIMEZONE:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        row.setColumn(columnIndex, eventRow.getTimestamp(columnName));
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.CLOB:
                        row.setColumn(columnIndex, eventRow.getString(columnName));
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        row.setColumn(columnIndex, eventRow.getString(columnName).getBytes(charset));
                        break;
                    case Types.NULL:
                        row.setColumn(columnIndex, null);
                        break;
                    case Types.BOOLEAN:
                        row.setColumn(columnIndex, eventRow.getBoolean(columnName));
                        break;
                    default:
                        throw new IllegalArgumentException("Column data type [" +
                                columnType.name + "] is not supported");
                }
            } else {
                row.setColumn(columnIndex, null);
            }

            ++columnIndex;
        }
    }

    @Override
    public void configure(Context context) {
        batchCount = context.getInteger(BATCH_COUNT);

        if (batchCount > MAX_BATCH_COUNT) {
            throw new FlumeException("Batch count must less than or equal 10000");
        }

        String charsetName = context.getString(ENCODING_PROP, DEFAULT_ENCODING);
        try {
            charset = Charset.forName(charsetName);
        } catch (IllegalArgumentException e) {
            throw new FlumeException(
                    String.format("Invalid or unsupported charset %s", charsetName), e);
        }

        adbpgTblName = context.getString(ADBPG_TBL_NAME);
        adbpgSchemaName = context.getString(ADBPG_SCHEMA_NAME);

        initAdbpgClient(context);
    }

    private void initAdbpgClient(Context context) {
        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setHost(context.getString(ADBPG_HOST));
        databaseConfig.setPort(context.getInteger(ADBPG_PORT));
        databaseConfig.setUser(context.getString(ADBPG_USER));
        databaseConfig.setPassword(context.getString(ADBPG_PASSWORD));
        databaseConfig.setDatabase(context.getString(ADBPG_DB));

        List<String> tables = new ArrayList<String>();
        tables.add(adbpgTblName);
        databaseConfig.addTable(tables, adbpgSchemaName);

        databaseConfig.setColumns(
                Collections.singletonList("*"),
                adbpgTblName,
                adbpgSchemaName
        );

        databaseConfig.setEmptyAsNull(false);
        databaseConfig.setInsertIgnore(true);
        databaseConfig.setRetryTimes(3);
        databaseConfig.setRetryIntervalTime(1000);
        adb4pgClient = new Adb4pgClient(databaseConfig);
    }

    @Override
    public synchronized void stop() {
        super.stop();

        adb4pgClient.forceStop();
    }
}
