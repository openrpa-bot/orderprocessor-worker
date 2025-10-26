package com.nigam.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

public class TemporalRunner {
    public static void main(String[] args) {

        System.setProperty("java.net.preferIPv4Stack", "true");
        //String temporalHost = System.getenv().getOrDefault("TEMPORAL_HOST", "temporal:7233");
        String temporalHost = System.getenv().getOrDefault("TEMPORAL_HOST", "192.168.1.112:7233");

        // 1️⃣ Connect to Temporal service (default: localhost:7233)

        WorkflowServiceStubs service = WorkflowServiceStubs.newInstance(
                WorkflowServiceStubsOptions.newBuilder()
                        .setTarget(temporalHost)
                        .build()
        );


        // 2️⃣ Create client
        WorkflowClient client = WorkflowClient.newInstance(service);

        // 3️⃣ Create worker factory & worker for our task queue
        WorkerFactory factory = WorkerFactory.newInstance(client);
        Worker worker = factory.newWorker("GREETING_TASK_QUEUE");

        // 4️⃣ Register workflow & activities
        worker.registerWorkflowImplementationTypes(GreetingWorkflowImpl.class);
        worker.registerActivitiesImplementations(new GreetingActivitiesImpl());

        // 5️⃣ Start worker
        factory.start();

        System.out.println("✅ Temporal worker started on GREETING_TASK_QUEUE...");
    }
}
