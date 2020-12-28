/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.delta.datastream;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.datastream.v1alpha1.DataStream;
import com.google.api.services.datastream.v1alpha1.model.DiscoverConnectionProfileRequest;
import com.google.api.services.datastream.v1alpha1.model.DiscoverConnectionProfileResponse;
import com.google.api.services.datastream.v1alpha1.model.OracleColumn;
import com.google.api.services.datastream.v1alpha1.model.OracleRdbms;
import com.google.api.services.datastream.v1alpha1.model.OracleSchema;
import com.google.api.services.datastream.v1alpha1.model.OracleTable;
import com.google.cloud.ServiceOptions;
import com.google.common.collect.Iterables;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.api.assessment.TableSummary;
import io.cdap.delta.datastream.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lists and describes tables.
 */
public class DatastreamTableRegistry implements TableRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatastreamTableRegistry.class);
  private final DatastreamConfig config;
  private final DataStream datastream;
  // parent path of datastream resources in form of "projects/projectId/locations/region"
  private final String parentPath;
  // TODO find a better way to get system schemas for different version of Oracle.
  //  May need additional support from datasteam for :
  //  1. get the version of oracle and we map it to system schemas
  //  or
  //  2. get information about whether the schema is a system schema
  private static final Set<String> SYSTEM_SCHEMA = new HashSet<>(Arrays.asList("SYS", "SYSTEM", "CTXSYS", "XDB",
    "MDSYS", "FLOWS_FILES", "APEX_040000", "OUTLN"));


  public DatastreamTableRegistry(DatastreamConfig config, DataStream datastream) {
    this.config = config;
    this.datastream = datastream;
    //TODO validate whether the region is valid

    this.parentPath =
      String.format("projects/%s/locations/%s", ServiceOptions.getDefaultProjectId(), config.getRegion());
  }

  @Override
  public TableList listTables() throws IOException {
    LOGGER.debug("List tables...");
    List<TableSummary> tables = new ArrayList<>();

    DiscoverConnectionProfileResponse response = discover();
    if (response.getOracleRdbms().getOracleSchemas() == null) {
      return new TableList(tables);
    }

    for (OracleSchema schema : response.getOracleRdbms().getOracleSchemas()) {
      String schemaName = schema.getSchemaName();
      if (SYSTEM_SCHEMA.contains(schemaName.toUpperCase())) {
        //skip system tables
        continue;
      }
      if (schema.getOracleTables() == null) {
        continue;
      }
      for (OracleTable table : schema.getOracleTables()) {
        String tableName = table.getTableName();
        tables.add(new TableSummary(config.getSid(), tableName,
          table.getOracleColumns() == null ? 0 : table.getOracleColumns().size(), schemaName));
      }
    }
    return new TableList(tables);
  }

  @Override
  public TableDetail describeTable(String db, String schema, String table) throws TableNotFoundException, IOException {
    LOGGER.debug(String.format("Describe table, db: %s, table: %s, schema: %s", db, table, schema));
    DiscoverConnectionProfileResponse discoverResponse;
    try {
      discoverResponse = discover(schema, table);
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == 404) {
        throw new TableNotFoundException(db, schema, table, e.getMessage(), e);
      }
      throw e;
    }

    OracleSchema oracleSchema =
      Iterables.getOnlyElement(discoverResponse.getOracleRdbms().getOracleSchemas());
    OracleTable oracleTable = Iterables.getOnlyElement(oracleSchema.getOracleTables());

    List<ColumnDetail> columns = new ArrayList<>(oracleTable.getOracleColumns().size());
    List<String> primaryKeys = new ArrayList<>();
    for (OracleColumn column : oracleTable.getOracleColumns()) {
      Map<String, String> properties = new HashMap<>();
      if (column.getPrecision() != null) {
        properties.put(DatastreamTableAssessor.PRECISION, Integer.toString(column.getPrecision()));
      }
      if (column.getScale() != null) {
        properties.put(DatastreamTableAssessor.SCALE, Integer.toString(column.getScale()));
      }
      columns.add(
        new ColumnDetail(column.getColumnName(), Utils.convertStringDataTypeToSQLType(column.getDataType()),
          Boolean.TRUE.equals(column.getNullable()), properties));
      if (Boolean.TRUE.equals(column.getPrimaryKey())) {
        primaryKeys.add(column.getColumnName());
      }
    }
    return new TableDetail.Builder(db, table, schema).setColumns(columns).setPrimaryKey(primaryKeys).build();
  }

  @Override
  public StandardizedTableDetail standardize(TableDetail tableDetail) {
    List<Schema.Field> columnSchemas = new ArrayList<>();
    for (ColumnDetail detail : tableDetail.getColumns()) {
      ColumnEvaluation evaluation = DatastreamTableAssessor.evaluateColumn(detail);
      if (evaluation.getAssessment().getSupport().equals(ColumnSupport.NO)) {
        continue;
      }
      columnSchemas.add(evaluation.getField());
    }
    Schema schema = Schema.recordOf("outputSchema", columnSchemas);
    return new StandardizedTableDetail(tableDetail.getDatabase(), tableDetail.getSchema(), tableDetail.getTable(),
      tableDetail.getPrimaryKey(), schema);
  }

  @Override
  public void close() throws IOException {
  }

  private DiscoverConnectionProfileResponse discover(String schema, String table) throws IOException {
    DiscoverConnectionProfileRequest request =
      new DiscoverConnectionProfileRequest().setConnectionProfile(Utils.buildOracleConnectionProfile(null, config))
        .setOracleRdbms(new OracleRdbms().setOracleSchemas(Arrays.asList(new OracleSchema().setSchemaName(schema)
          .setOracleTables(Arrays.asList(new OracleTable().setTableName(table))))));
    return datastream.projects().locations().connectionProfiles().discover(parentPath, request).execute();
  }

  private DiscoverConnectionProfileResponse discover() throws IOException {
    DiscoverConnectionProfileRequest request =
      new DiscoverConnectionProfileRequest().setConnectionProfile(Utils.buildOracleConnectionProfile(null, config))
        .setRecursive(true);
    return datastream.projects().locations().connectionProfiles().discover(parentPath, request).execute();
  }
}