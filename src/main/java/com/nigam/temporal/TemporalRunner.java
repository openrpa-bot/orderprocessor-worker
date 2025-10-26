package com.nigam.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class TemporalRunner {

      public static void main(String[] args) {
            System.out.println("🚀 Starting TemporalWorker...");

            try {
                  // ✅ Force IPv4 stack
                  System.setProperty("java.net.preferIPv4Stack", "true");
                  System.setProperty("io.netty.resolver.dns.native", "false"); // Force Netty to use JVM DNS resolver
                  System.out.println("✅ IPv4 stack enforced and Netty DNS set to JVM resolver");

                  // Get Temporal host from environment or fallback
                  String temporalHost = System.getenv().getOrDefault("TEMPORAL_HOST", "192.168.1.112:7233");
                  System.out.println("🌐 Temporal host configured as: " + temporalHost);

                  String[] parts = temporalHost.split(":");
                  if (parts.length != 2) {
                        throw new IllegalArgumentException("TEMPORAL_HOST must be in format <host>:<port>");
                  }
                  String host = parts[0];
                  int port = Integer.parseInt(parts[1]);
                  System.out.println("🔍 Parsed host: " + host + ", port: " + port);

                  // Optional: resolve host IP to verify IPv4
                  try {
                        InetAddress inetAddress = InetAddress.getByName(host);
                        System.out.println("🔗 Resolved host IP: " + inetAddress.getHostAddress());
                  } catch (UnknownHostException e) {
                        System.err.println("⚠️ Failed to resolve host: " + host);
                        e.printStackTrace();
                  }

                  // Build gRPC channel
                  System.out.println("⚡ Creating gRPC channel...");
                  ManagedChannel channel = NettyChannelBuilder
                          .forAddress(host, port)
                          .usePlaintext()
                          .build();
                  System.out.println("✅ gRPC channel created");

                  // Connect to Temporal service
                  System.out.println("⚡ Connecting to Temporal service...");
                  WorkflowServiceStubs service = WorkflowServiceStubs.newInstance(
                          WorkflowServiceStubsOptions.newBuilder()
                                  .setChannel(channel)
                                  .build()
                  );
                  System.out.println("✅ Connected to Temporal service");

                  // Create client
                  System.out.println("⚡ Creating WorkflowClient...");
                  WorkflowClient client = WorkflowClient.newInstance(service);
                  System.out.println("✅ WorkflowClient created");

                  // Create worker factory and worker
                  System.out.println("⚡ Creating WorkerFactory and Worker...");
                  WorkerFactory factory = WorkerFactory.newInstance(client);
                  Worker worker = factory.newWorker("GREETING_TASK_QUEUE");
                  System.out.println("✅ Worker created for task queue: GREETING_TASK_QUEUE");

                  // Register workflow and activities
                  System.out.println("⚡ Registering workflows and activities...");
                  worker.registerWorkflowImplementationTypes(GreetingWorkflowImpl.class);
                  worker.registerActivitiesImplementations(new GreetingActivitiesImpl());
                  System.out.println("✅ Workflows and activities registered");

                  // Start worker
                  System.out.println("⚡ Starting WorkerFactory...");
                  factory.start();
                  System.out.println("✅ Temporal worker started successfully on GREETING_TASK_QUEUE");

                  // Add shutdown hook
                  Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        System.out.println("🛑 Shutting down Temporal worker...");
                        service.shutdown();
                        channel.shutdownNow();
                  }));

            } catch (Exception e) {
                  System.err.println("❌ TemporalWorker failed to start");
                  e.printStackTrace();
            }
      }
}
