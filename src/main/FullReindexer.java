/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.commons.cli.*;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FullReindexer {

  static final int NUM_ROWS = 50;
  static final String ID_FIELD = "id";

  static Options options = new Options();

  static {
    final Option sourceOp = Option.builder("s")
        .longOpt("source")
        .argName("source")
        .desc("Name of the source collection")
        .required()
        .hasArg(true)
        .build();
    final Option destOp = Option.builder("d")
        .argName("dest")
        .longOpt("dest")
        .desc("Name of the target collection")
        .required()
        .hasArg(true)
        .longOpt("destination")
        .build();
    final Option zkStringOp = Option.builder("z")
        .argName("zk")
        .longOpt("zk")
        .desc("ZooKeeper host string")
        .required()
        .hasArg(true)
        .build();
    final Option totalThreadsOp = Option.builder("n")
        .argName("totalThreads")
        .desc("Total #threads")
        .hasArg(true)
        .longOpt("totalThreads")
        .build();
    options.addOption(sourceOp)
        .addOption(destOp)
        .addOption(zkStringOp)
        .addOption(totalThreadsOp);
  }


  final String sourceCollection;
  final String destCollection;
  final String zkHost;
  final int totalThreads;

  CloudSolrClient readClient;
  CloudSolrClient writeClient;

  public FullReindexer(String sourceCollection, String destCollection, String zkHost, int totalThreads) {
    this.sourceCollection = sourceCollection;
    this.destCollection = destCollection;
    this.zkHost = zkHost;
    this.totalThreads = totalThreads;

    readClient = new CloudSolrClient(zkHost);
    readClient.setDefaultCollection(sourceCollection);

    writeClient = new CloudSolrClient(zkHost);
    writeClient.setDefaultCollection(destCollection);
  }

  public void reindex() throws IOException {
    ExecutorService executor = Executors.newFixedThreadPool(totalThreads);

    for(int i = 0; i < totalThreads ; i++) {
      Worker worker = new Worker(i, totalThreads, readClient, writeClient);
      executor.execute(worker);
    }

    executor.shutdown();

    while(!executor.isTerminated()) {
      // Wait until done
    }

    readClient.close();
    writeClient.close();
  }


  /**
   * Class that actually does the work
   */
  class Worker implements Runnable {
    final int id;
    final int totalThreads;
    final CloudSolrClient readClient;
    final CloudSolrClient writeClient;

    Worker(int id, int totalThreads, CloudSolrClient readClient, CloudSolrClient writeClient) {
      this.id = id;
      this.totalThreads = totalThreads;
      this.readClient = readClient;
      this.writeClient = writeClient;
    }

    @Override
    public void run() {
      SolrQuery solrQuery = new SolrQuery();
      solrQuery.setRows(NUM_ROWS);
      solrQuery.setQuery("*:*");
      solrQuery.add("fq","{!hash workers=" + totalThreads + " worker=" + id + "}");
      solrQuery.add("partitionKeys", ID_FIELD);
      solrQuery.addSort("id", SolrQuery.ORDER.asc);  // Pay attention to this line
      String cursorMark = CursorMarkParams.CURSOR_MARK_START;
      boolean done = false;

      try {
        while (!done) {
          solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
          QueryResponse rsp = readClient.query(solrQuery);
          String nextCursorMark = rsp.getNextCursorMark();
          for (SolrDocument d : rsp.getResults()) {
            System.out.println("Worker[" + id + "]: " + d.toString());
            d.removeFields("_version_");
            SolrInputDocument inputDocument = ClientUtils.toSolrInputDocument(d);
            writeClient.add(inputDocument);
          }
          if (cursorMark.equals(nextCursorMark)) {
            done = true;
          }
          cursorMark = nextCursorMark;
        }
        writeClient.commit();
      } catch (SolrServerException | IOException e) {
        // Print exception
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws IOException {
    if(args == null || args.length == 0) {
      help();
      System.exit(1);
    }

    final CommandLine cli = parseCli(args);

    FullReindexer fullReindexer = new FullReindexer(cli.getOptionValue("source"),
        cli.getOptionValue("dest"),
        cli.getOptionValue("zk"),
        Integer.parseInt(cli.getOptionValue("n")));

    System.out.println("Starting to reindex.");

    fullReindexer.reindex();

    System.out.println("Completed reindexing.");

  }

  private static CommandLine parseCli(String[] args) {
    final CommandLineParser parser = new DefaultParser();
    CommandLine cli = null;
    try {
      cli = parser.parse(options, args);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return cli;
  }

  private static void help() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("java FullReindexer", options);
  }

}
