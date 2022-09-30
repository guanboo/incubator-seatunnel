/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.druid.client;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.druid.config.DruidSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.druid.config.DruidTypeMapper;

import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;

@Data
public class DruidInputFormat implements Serializable {
    protected static final String COLUMNS_DEFAULT = "*";
    protected static final String QUERY_TEMPLATE = "SELECT %s FROM %s WHERE 1=1";
    private static final Logger LOGGER = LoggerFactory.getLogger(DruidInputFormat.class);
    protected transient Connection connection;
    protected transient PreparedStatement statement;
    protected transient ResultSet resultSet;
    protected SeaTunnelRowType rowTypeInfo;
    protected DruidSourceOptions druidSourceOptions;
    protected String quarySQL;
    protected boolean hasNext;

    public DruidInputFormat(DruidSourceOptions druidSourceOptions) {
        this.druidSourceOptions = druidSourceOptions;
        this.rowTypeInfo = initTableField();
    }

    public ResultSetMetaData getResultSetMetaData() throws SQLException {
        try {
            quarySQL = getSQL();
            connection = DriverManager.getConnection(druidSourceOptions.getUrl());
            statement = connection.prepareStatement(quarySQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            return statement.getMetaData();
        } catch (SQLException se) {
            throw new SQLException("ResultSetMetaData() failed." + ExceptionUtils.getMessage(se), se);
        }
    }

    public void openInputFormat() {
        try {
            connection = DriverManager.getConnection(druidSourceOptions.getUrl());
            statement = connection.prepareStatement(quarySQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new IllegalArgumentException("openInputFormat() failed." + ExceptionUtils.getMessage(se), se);
        }
    }

    private String getSQL() throws SQLException {
        String columns = COLUMNS_DEFAULT;
        String startTimestamp = druidSourceOptions.getStartTimestamp();
        String endTimestamp = druidSourceOptions.getEndTimestamp();
        String dataSource = druidSourceOptions.getDatasource();
        if (CollectionUtils.isEmpty(druidSourceOptions.getColumns()) && druidSourceOptions.getColumns().size() > 0) {
            columns = String.join(",", druidSourceOptions.getColumns());
        }
        String sql = String.format(QUERY_TEMPLATE, columns, dataSource);
        if (startTimestamp != null) {
            sql += " AND __time >=  '" + startTimestamp + "'";
        }
        if (endTimestamp != null) {
            sql += " AND __time <  '" + endTimestamp + "'";
        }
        return sql;
    }

    public void closeInputFormat() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException se) {
            LOGGER.error("DruidInputFormat Statement couldn't be closed", se);
        } finally {
            statement = null;
            resultSet = null;
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException se) {
                LOGGER.error("DruidInputFormat Connection couldn't be closed", se);
            } finally {
                connection = null;
            }
        }
    }

    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    public SeaTunnelRow nextRecord() throws IOException {
        try {
            if (!hasNext) {
                return null;
            }
            SeaTunnelRow seaTunnelRow = toInternal(resultSet, rowTypeInfo);
            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return seaTunnelRow;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + ExceptionUtils.getMessage(se), se);
        } catch (NullPointerException npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    public SeaTunnelRow toInternal(ResultSet rs, SeaTunnelRowType rowTypeInfo) throws SQLException {
        SeaTunnelDataType<?>[] seaTunnelDataTypes = rowTypeInfo.getFieldTypes();
        Object[] fields = new Object[seaTunnelDataTypes.length];

        for (int i = 1; i <= seaTunnelDataTypes.length; i++) {
            Object seatunnelField;
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i - 1];
            seaTunnelDataType.getSqlType();
            if (null == rs.getObject(i)) {
                seatunnelField = null;
            } else {
                switch (seaTunnelDataType.getSqlType()) {
                    case BOOLEAN:
                        seatunnelField = rs.getBoolean(i);
                        break;
                    case  BYTES:
                        seatunnelField = rs.getByte(i);
                        break;
                    case SMALLINT:
                        seatunnelField = rs.getShort(i);
                        break;
                    case INT:
                        seatunnelField = rs.getInt(i);
                        break;
                    case BIGINT:
                        seatunnelField = rs.getLong(i);
                        break;
                    case FLOAT:
                        seatunnelField = rs.getFloat(i);
                        break;
                    case DOUBLE:
                        seatunnelField = rs.getDouble(i);
                        break;
                    case STRING:
                        seatunnelField = rs.getString(i);
                        break;
                    case TIMESTAMP:
                        Timestamp ts = rs.getTimestamp(i, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                        LocalDateTime localDateTime = LocalDateTime.ofInstant(ts.toInstant(), ZoneId.of("UTC"));  // good
                        seatunnelField = localDateTime;
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + seaTunnelDataType);
                }
            }
            fields[i - 1] = seatunnelField;
        }

        return new SeaTunnelRow(fields);
    }

    private SeaTunnelRowType initTableField() {
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData = getResultSetMetaData();
            String[] fieldNames = new String[resultSetMetaData.getColumnCount()];
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                fieldNames[i - 1] = resultSetMetaData.getColumnName(i);
                seaTunnelDataTypes.add(DruidTypeMapper.DRUID_TYPE_MAPPS.get(resultSetMetaData.getColumnTypeName(i)));
            }
            rowTypeInfo = new SeaTunnelRowType(fieldNames, seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[seaTunnelDataTypes.size()]));
        } catch (SQLException e) {
            LOGGER.warn("get row type info exception", e);
        }

        return rowTypeInfo;
    }
}
