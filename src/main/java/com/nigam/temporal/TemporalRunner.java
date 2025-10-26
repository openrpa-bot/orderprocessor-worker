package com.nigam.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.grpc.ManagedChannel;
import io.grpc.LoadBalancerProvider;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ServiceLoader;

public class TemporalRunner {

      public static void main(String[] args) {
            System.out.println("🚀 Starting TemporalWorker...");

            try {
                  // Force IPv4 stack
                  System.setProperty("java.net.preferIPv4Stack", "true");
                  System.setProperty("io.netty.resolver.dns.native", "false");
                  System.out.println("✅ IPv4 stack enforced and Netty DNS set to JVM resolver");

                  // Log system properties
                  System.out.println("ℹ️ Java version: " + System.getProperty("java.version"));
                  System.out.println("ℹ️ Java vendor: " + System.getProperty("java.vendor"));
                  System.out.println("ℹ️ OS name: " + System.getProperty("os.name"));
                  System.out.println("ℹ️ OS arch: " + System.getProperty("os.arch"));

                  // Check for gRPC LoadBalancerProviders
                  System.out.println("🔎 Registered gRPC LoadBalancerProviders:");
                  ServiceLoader<LoadBalancerProvider> loader = ServiceLoader.load(LoadBalancerProvider.class);
                  boolean found = false;
                  for (LoadBalancerProvider provider : loader) {
                        found = true;
                        System.out.println("   - " + provider.getClass().getName() + " (policy: " + provider.getPolicyName() + ")");
                  }
                  if (!found) {
                        System.out.println("⚠️ No LoadBalancerProvider found! 'pick_first' may be missing from classpath.");
                  }

                  // Get Temporal host
                  String temporalHost = System.getenv().getOrDefault("TEMPORAL_HOST", "192.168.1.112:7233");
                  System.out.println("🌐 Temporal host configured as: " + temporalHost);

                  String[] parts = temporalHost.split(":");
                  if (parts.length != 2) throw new IllegalArgumentException("TEMPORAL_HOST must be in format <host>:<port>");
                  String host = parts[0];
                  int port = Integer.parseInt(parts[1]);
                  System.out.println("🔍 Parsed host: " + host + ", port: " + port);

                  // Resolve host
                  try {
                        InetAddress inetAddress = InetAddress.getByName(host);
                        System.out.println("🔗 Resolved host IP: " + inetAddress.getHostAddress());
                  } catch (UnknownHostException e) {
                        System.err.println("⚠️ Failed to resolve host: " + host);
                        e.printStackTrace();
                  }

                  // Build gRPC channel with pick_first explicitly
                  System.out.println("⚡ Creating gRPC channel with pick_first load balancer...");
                  ManagedChannel channel = NettyChannelBuilder.forAddress(host, port)
                          .defaultLoadBalancingPolicy("pick_first")
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

                  // Create WorkflowClient
                  System.out.println("⚡ Creating WorkflowClient...");
                  WorkflowClient client = WorkflowClient.newInstance(service);
                  System.out.println("✅ WorkflowClient created");

                  // Create WorkerFactory and Worker
                  System.out.println("⚡ Creating WorkerFactory and Worker...");
                  WorkerFactory factory = WorkerFactory.newInstance(client);
                  Worker worker = factory.newWorker("GREETING_TASK_QUEUE");
                  System.out.println("✅ Worker created for task queue: GREETING_TASK_QUEUE");

                  // Register workflow and activities
                  System.out.println("⚡ Registering workflows and activities...");
                  worker.registerWorkflowImplementationTypes(GreetingWorkflowImpl.class);
                  worker.registerActivitiesImplementations(new GreetingActivitiesImpl());
                  System.out.println("✅ Workflows and activities registered");

                  // Start WorkerFactory
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
