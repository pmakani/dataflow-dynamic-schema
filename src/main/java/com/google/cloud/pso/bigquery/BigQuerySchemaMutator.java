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

package com.google.cloud.pso.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.BigQuery;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;

/**
 * The {@link BigQuerySchemaMutator} class is a {@link PTransform} which given a PCollection of
 * TableRows, will compare the TableRows to an existing table schema in order to dynamically perform
 * schema updates on the output table.
 */
@AutoValue
public abstract class BigQuerySchemaMutator
    extends PTransform<PCollection<TableRow>, PCollection<TableRowWithSchema>> {

  @Nullable
  abstract BigQuery getBigQueryService();

  abstract PCollectionView<Map<Integer, TableRowWithSchema>> getIncomingRecordsView();

  abstract Builder toBuilder();

  /** The builder for the {@link BigQuerySchemaMutator} class. */
  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setBigQueryService(BigQuery bigQuery);

    abstract Builder setIncomingRecordsView(
        PCollectionView<Map<Integer, TableRowWithSchema>> incomingRecordsView);

    abstract BigQuerySchemaMutator build();
  }

  public static BigQuerySchemaMutator mutateWithSchema(
      PCollectionView<Map<Integer, TableRowWithSchema>> incomingRecordsView) {
    return new com.google.cloud.pso.bigquery.AutoValue_BigQuerySchemaMutator.Builder()
        .setIncomingRecordsView(incomingRecordsView)
        .build();
  }

  /**
   * @param bigQuery
   * @return
   */
  public BigQuerySchemaMutator withBigQueryService(BigQuery bigQuery) {
    return toBuilder().setBigQueryService(bigQuery).build();
  }

  @Override
  public PCollection<TableRowWithSchema> expand(PCollection<TableRow> input) {

    // Here we'll key every failed record by the same key so we can batch the mutations being made
    // to BigQuery. The batch of records will then be passed to a schema mutator so the schema of
    // those records can be updated.
    PCollection<TableRowWithSchema> mutatedRecords =
        input
            .apply("1mWindow", Window.into(FixedWindows.of(Duration.standardMinutes(1L))))
            .apply(
                "FailedInsertToTableRowWithSchema",
                ParDo.of(new FailedInsertToTableRowWithSchema(getIncomingRecordsView()))
                    .withSideInputs(getIncomingRecordsView()))
            .setCoder(TableRowWithSchemaCoder.of())
            .apply("KeyRecords", WithKeys.of("failed-record-batch"))
            .apply("GroupRecords", GroupByKey.create())
            .apply("RemoveKey", Values.create())
            .apply("MutateSchema", ParDo.of(new TableRowSchemaMutator(getBigQueryService())));

    return mutatedRecords;
  }
}
