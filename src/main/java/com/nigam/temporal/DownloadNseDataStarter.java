package com.nigam.temporal;

import com.nigam.temporal.nsedata.DownloadNseDataInput;
import com.nigam.temporal.nsedata.DownloadNseDataWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Starts the DownloadNseData workflow. Run this while TemporalRunner (worker) is running
 * so the workflow is picked up from the "downloadNseData" task queue.
 *
 * Usage:
 *   ./gradlew runDownloadNseDataStarter
 *   ./gradlew runDownloadNseDataStarter -PtaskType=allIndices
 *
 * Or with Java:
 *   java -cp ... com.nigam.temporal.DownloadNseDataStarter allIndices
 *
 * Requires TEMPORAL_HOST (default 192.168.1.112:7233) and a running worker on task queue "downloadNseData".
 */
public class DownloadNseDataStarter {

  private static final Logger log = LogManager.getLogger(DownloadNseDataStarter.class);

  private static final String TASK_QUEUE = "downloadNseData";
  private static final String DEFAULT_TASK_TYPE = "allIndices";

  public static void main(String[] args) {
    String taskType = (args != null && args.length > 0 && args[0] != null && !args[0].isEmpty())
        ? args[0].trim()
        : DEFAULT_TASK_TYPE;

    log.info("DownloadNseDataStarter main() started, taskType={}, argsLength={}", taskType, args != null ? args.length : 0);

    System.setProperty("java.net.preferIPv4Stack", "true");
    String temporalHost = System.getenv().getOrDefault("TEMPORAL_HOST", "192.168.1.112:7233");
    log.info("DownloadNseDataStarter TEMPORAL_HOST={}", temporalHost);
    String[] parts = temporalHost.split(":");
    if (parts.length != 2) {
      log.error("DownloadNseDataStarter invalid TEMPORAL_HOST: {}", temporalHost);
      throw new IllegalArgumentException("TEMPORAL_HOST must be <host>:<port>, got: " + temporalHost);
    }
    String host = parts[0];
    int port = Integer.parseInt(parts[1]);
    log.info("DownloadNseDataStarter parsed host={}, port={}", host, port);

    log.info("DownloadNseDataStarter building gRPC channel");
    ManagedChannel channel = NettyChannelBuilder.forAddress(host, port)
        .defaultLoadBalancingPolicy("pick_first")
        .usePlaintext()
        .build();
    log.info("DownloadNseDataStarter gRPC channel created");

    log.info("DownloadNseDataStarter connecting to Temporal service");
    WorkflowServiceStubs service = WorkflowServiceStubs.newInstance(
        WorkflowServiceStubsOptions.newBuilder().setChannel(channel).build()
    );
    WorkflowClient client = WorkflowClient.newInstance(service);
    log.info("DownloadNseDataStarter WorkflowClient created");

    String workflowId = "download-nse-" + taskType + "-" + System.currentTimeMillis();
    log.info("DownloadNseDataStarter workflowId={}, taskQueue={}", workflowId, TASK_QUEUE);
    WorkflowOptions options = WorkflowOptions.newBuilder()
        .setTaskQueue(TASK_QUEUE)
        .setWorkflowId(workflowId)
        .build();

    DownloadNseDataWorkflow workflow = client.newWorkflowStub(DownloadNseDataWorkflow.class, options);
    DownloadNseDataInput input = new DownloadNseDataInput(taskType);
    log.info("DownloadNseDataStarter workflow stub and input created, starting workflow execution");

    try {
      String result = workflow.downloadNseData(input);
      log.info("DownloadNseDataStarter workflow finished successfully: result={}", result);
    } catch (Exception e) {
      log.error("DownloadNseDataStarter workflow failed: {}", e.getMessage(), e);
      throw e;
    } finally {
      log.info("DownloadNseDataStarter shutting down client and channel");
      service.shutdown();
      channel.shutdownNow();
      log.info("DownloadNseDataStarter shutdown complete");
    }
  }
}
