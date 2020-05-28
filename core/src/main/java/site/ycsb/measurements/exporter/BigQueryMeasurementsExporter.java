/**
 * Copyright 2020 Google LLC.
 * This software is provided as-is, without warranty or
 * representation for any use or purpose. Your use of it is subject to your agreement
 * with Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.measurements.exporter;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;

import com.google.cloud.bigquery.TableId;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Export measurements into BigQuery.
 * Write measurements to tempFile using JSONMeasurementsExporter then read it
 * and send to BigQuery in the close() function
 */
public class BigQueryMeasurementsExporter extends JSONArrayMeasurementsExporter {

  private static BigQuery bq;

  // BigQuery details
  private static final String PROJECT_ID_PARAM_NAME = "google.bigquery.project.id";

  private String location;
  private static final String LOCATION_PARAM_NAME = "google.bigquery.location";

  private String dataset;
  private static final String DATASET_PARAM_NAME = "google.bigquery.dataset";

  private String table;
  private static final String TABLE_PARAM_NAME = "google.bigquery.table";
  private static final String DEFAULT_TABLE = "measurements";

  private String jobName;
  private static final String JOB_NAME_PARAM = "jobname";
  private static final String DEFAULT_JOB_NAME = "ycsb";

  // Temporary location to write JSON
  private static final String TEMP_FILE_PARAM_NAME = "bqTempFile";
  private static final String DEFAULT_TEMP_FILE = "/tmp/bq_upload.json";

  private Properties props;

  private static OutputStream getTempFileStream(Properties exportProperties) throws IOException {
    String tempFilename = (String) exportProperties.getOrDefault(TEMP_FILE_PARAM_NAME, DEFAULT_TEMP_FILE);
    return new FileOutputStream(tempFilename);
  }

  public BigQueryMeasurementsExporter(Properties exportProperties) throws IOException, RuntimeException {
    // Hand a tempFile to the parent.
    super(getTempFileStream(exportProperties));

    System.out.println("Initializing BigQueryExporter");

    // Set up BigQuery connection
    props = exportProperties;
    String projectId = (String) props.get(PROJECT_ID_PARAM_NAME);
    if (projectId == null) {
      throw new RuntimeException(
          "Missing required property for BigQueryExporter " + PROJECT_ID_PARAM_NAME);
    }
    location = (String) props.get(LOCATION_PARAM_NAME);
    if (location == null) {
      throw new RuntimeException(
          "Missing required property for BigQueryExporter " + DATASET_PARAM_NAME);
    }
    dataset = (String) props.get(DATASET_PARAM_NAME);
    if (dataset == null) {
      throw new RuntimeException(
          "Missing required property for BigQueryExporter " + DATASET_PARAM_NAME);
    }
    table = (String) props.getOrDefault(TABLE_PARAM_NAME, DEFAULT_TABLE);
    jobName = (String) props.getOrDefault(JOB_NAME_PARAM, DEFAULT_JOB_NAME);

    // Initialize BigQuery connection
    bq = BigQueryOptions.newBuilder().setLocation(location).build().getService();
  }

  @Override
  public void close() throws IOException, RuntimeException {
    super.close();

    // Get the file that the parent created
    String tempFilename = (String) props.getOrDefault(TEMP_FILE_PARAM_NAME, DEFAULT_TEMP_FILE);

    // Start the Insert Request
    TableId tableId = TableId.of(dataset, table);
    InsertAllRequest.Builder request = InsertAllRequest.newBuilder(tableId);
    Map<String, Object> rowContent = new HashMap<>();

    // Read the JSON into the Insert Request
    JsonParser parser = new JsonFactory().createJsonParser(new FileReader(tempFilename));
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode node = objectMapper.readTree(parser);
    Iterator<JsonNode> iterator = node.getElements();
    while (iterator.hasNext()) {
      JsonNode j = iterator.next();
      rowContent.put("jobname", jobName);
      rowContent.put("metric", j.get("metric").asText());
      rowContent.put("measurement", j.get("measurement").asText());
      rowContent.put("value", j.get("value").asDouble());
      request.addRow(rowContent);
    }

    // Done
    System.out.println("Sending metrics to BigQuery");
    InsertAllResponse response = bq.insertAll(request.build());
    if (response.hasErrors()) {
      throw new RuntimeException((Throwable) response.getInsertErrors());
    }
  }
}
