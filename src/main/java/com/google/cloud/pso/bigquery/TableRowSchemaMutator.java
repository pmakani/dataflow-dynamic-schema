/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.pso.bigquery;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.Sets;
import org.apache.beam.sdk.transforms.DoFn;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/** The {@link TableRowWithSchema} mutator. */
public class TableRowSchemaMutator extends DoFn<Iterable<TableRowWithSchema>, TableRowWithSchema> {

  private transient BigQuery bigQuery;
  private transient BigQueryOptions options;

  public TableRowSchemaMutator(BigQuery bigQuery) {
    this.bigQuery = bigQuery;
  }

  @Setup
  public void setup() throws IOException {
    if (bigQuery == null) {
      this.options = BigQueryOptions.getDefaultInstance();
      bigQuery = options.getService();
    }
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    Iterable<TableRowWithSchema> mutatedRows = context.element();

    // Retrieve the table schema
    TableId tableId = TableId.of(options.getProjectId(), "beam_examples", "dynamic_schema");
    Table table = bigQuery.getTable(tableId);

    checkNotNull(table, "Failed to find table to mutate: " + tableId.toString());

    TableDefinition tableDef = table.getDefinition();
    Schema schema = tableDef.getSchema();

    checkNotNull(table, "Unable to retrieve schema for table: " + tableId.toString());

    // Compare the records to the known table schema
    Set<Field> additionalFields = Sets.newHashSet();
    for (TableRowWithSchema tableRowSchema : mutatedRows) {
      TableSchema tableSchema = tableRowSchema.getTableSchema();
      Iterator it = tableSchema.getFields().iterator();
      while (it.hasNext()) {
        Map<String, String> fieldMap = (Map<String, String>) it.next();
        String name = null;
        String type = null;
        for (Map.Entry<String, String> field : fieldMap.entrySet()) {
          if (field.getKey().equals("name")) {
            name = field.getValue();
          } else {
            type = field.getValue();
          }
        }
        if (name != null && type != null) {
          additionalFields.add(
                  Field.of(name, LegacySQLTypeName.valueOf(type))
                          .toBuilder()
                          .setMode(Mode.NULLABLE)
                          .build());
        }
      }
    }
    // Update the table schema for the new fields
    schema = Schema.of(additionalFields);
    table
        .toBuilder()
        .setDefinition(tableDef.toBuilder().setSchema(schema).build())
        .build()
        .update();

    // Pass all rows downstream now that the schema of the output table has been mutated.
    mutatedRows.forEach(context::output);
  }
}
