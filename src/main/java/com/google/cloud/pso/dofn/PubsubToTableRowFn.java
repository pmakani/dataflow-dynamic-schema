/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pso.dofn;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.pso.bigquery.BigQueryAvroUtils;
import com.google.cloud.pso.bigquery.TableRowWithSchema;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link DoFn} that converts a {@link PubsubMessage} with an Avro payload to a {@link
 * TableRowWithSchema} object.
 *
 * <p>The schema for the {@link TableRow} is inferred by inspecting and converting the Avro message
 * schema. By default, this function will set the namespace as the record's associated output table
 * for dynamic routing within the {@link org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO} sink using
 * {@link org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations}.
 */
public class PubsubToTableRowFn extends DoFn<PubsubMessage, TableRowWithSchema> {

  public static final TupleTag<TableRowWithSchema> MAIN_OUT = new TupleTag<TableRowWithSchema>() {};
  public static final TupleTag<PubsubMessage> DEADLETTER_OUT = new TupleTag<PubsubMessage>() {};

  @ProcessElement
  public void processElement(ProcessContext context) {
    PubsubMessage message = context.element();
    List<TableFieldSchema> fields = new ArrayList<>();
    for (Map.Entry<String, String> schema : message.getAttributeMap().entrySet()) {
      fields.add(new TableFieldSchema().setName(schema.getKey()).setType(schema.getValue()));
    }
    TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(fields);
    try (InputStream in = new ByteArrayInputStream(message.getPayload())) {
      TableRow row = TableRowJsonCoder.of().decode(in, Coder.Context.OUTER);
      context.output(
          MAIN_OUT,
          TableRowWithSchema.newBuilder()
              .setTableName("dynamic_schema")
              .setTableRow(row)
              .setTableSchema(tableSchema)
              .build());
    } catch (Exception e) {
      e.printStackTrace();
      context.output(DEADLETTER_OUT, message);
    }
  }
}
