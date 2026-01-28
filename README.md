# Order Processor Worker

A Temporal workflow worker for processing orders using the GreetingWorkflow.

## Prerequisites

- Java 21
- Temporal Server running (default: `192.168.1.112:7233` or set `TEMPORAL_HOST` environment variable)
- Gradle

## Running the Worker

Start the worker to listen for workflow tasks:

```bash
./gradlew run
```

Or build and run the JAR:

```bash
./gradlew shadowJar
java -jar build/libs/order-management-system-1.0-all.jar
```

## Starting Workflows from Temporal UI

To start a workflow through the Temporal UI:

1. **Open Temporal UI** (typically at `http://localhost:8088` or your Temporal server's UI endpoint)

2. **Navigate to Workflows** section

3. **Click "Start Workflow"** button

4. **Fill in the workflow details:**
   - **Workflow Type**: `GreetingWorkflow`
   - **Task Queue**: `GREETING_TASK_QUEUE`
   - **Workflow ID**: (optional, leave empty for auto-generated ID, or use a new ID to avoid replay issues)
   - **Input**: You can enter the name in either format:
     - As a JSON array: `["World"]` or `["John Doe"]`
     - As a JSON string: `"World"` or `"John Doe"`

5. **Click "Start"** to execute the workflow

### Example Input Values

The workflow accepts both formats:
- Array format: `["Alice"]`, `["Bob"]`, `["Your Name Here"]`
- String format: `"Alice"`, `"Bob"`, `"Your Name Here"`

The workflow will return: `"Hello, <name>!"`

**Note**: The workflow uses a custom deserializer that handles both string and array input formats, making it flexible for different Temporal UI input methods.

## Workflow Details

- **Workflow Type**: `GreetingWorkflow`
- **Task Queue**: `GREETING_TASK_QUEUE`
- **Input Type**: `String` (the name to greet)
- **Output Type**: `String` (the greeting message)

## Configuration

Set the `TEMPORAL_HOST` environment variable to point to your Temporal server:
```bash
export TEMPORAL_HOST=your-host:7233
```

Default is `192.168.1.112:7233`.

## LTP Calculator Workflow

A new workflow for calculating LTP (Last Traded Price) using OpenAlgo API.

### Workflow Details

- **Workflow Type**: `LtpCalculatorWorkflow`
- **Task Queue**: `ltpCalculator`
- **Input Parameters**:
  - `serverName`: Name of the server
  - `serverIP`: IP address of the server
  - `port`: Port number
  - `apiKey`: API key for OpenAlgo
  - `indexName`: Index name (e.g., "NIFTY")
  - `exchange`: Exchange name (e.g., "NSE_INDEX", default: "NSE_INDEX")
  - `expiry`: Expiry date (e.g., "27JAN26")
  - `strikeRange`: Strike range (integer, default: 10)

### Starting LTP Calculator Workflow from Temporal UI

1. **Open Temporal UI** and navigate to Workflows section
2. **Click "Start Workflow"**
3. **Fill in the workflow details:**
   - **Workflow Type**: `LtpCalculatorWorkflow`
   - **Task Queue**: `ltpCalculator`
   - **Workflow ID**: (optional)
   - **Input**: Provide input as JSON object:
     ```json
     {
       "serverName": "MyServer",
       "serverIP": "127.0.0.1",
       "port": "5000",
       "apiKey": "your_api_key_here",
       "indexName": "NIFTY",
       "exchange": "NSE_INDEX",
       "expiry": "27JAN26",
       "strikeRange": 10,
       "apiCallPauseMs": 500
     }
     ```
     Or as an array (matching parameter order - first 5 required, last 3 optional with defaults):
     ```json
     ["MyServer", "127.0.0.1", "5000", "your_api_key_here", "NIFTY", "NSE_INDEX", "27JAN26", 10]
     ```
     
     **Note**: 
     - `exchange` defaults to "NSE_INDEX", `strikeRange` defaults to 10 if not provided. `expiry` is required.
     - `apiCallPauseMs`: Pause between API calls in milliseconds to avoid throttling (default: 500ms)

4. **Click "Start"** to execute the workflow

The workflow will call the OpenAlgo `optionchain` API and return the status, underlying, and ATM strike information.

### Redis Storage

The workflow stores the API response in Redis with the key format:
```
openalgo:{serverName}:{indexName}:{expiry}
```

For example: `openalgo:Angel:NIFTY:27JAN26`

**Redis Configuration:**
Set the following environment variables to configure Redis connection:
- `REDIS_HOST`: Redis server host (default: `localhost`)
- `REDIS_PORT`: Redis server port (default: `6379`)
- `REDIS_PASSWORD`: Redis password (optional, leave unset if no password)

**Database Configuration (PostgreSQL/Citus):**
Set the following environment variables to configure database connection:
- `DB_HOST`: Database server host (default: `localhost`)
- `DB_PORT`: Database server port (default: `5432`)
- `DB_NAME`: Database name (default: `pgdb`)
- `DB_USER`: Database user (default: `pguser`)
- `DB_PASSWORD`: Database password (default: `pgpass`)

Example:
```bash
export REDIS_HOST=192.168.1.112
export REDIS_PORT=6379
export REDIS_PASSWORD=your_password

export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=openalgo
export DB_USER=postgres
export DB_PASSWORD=your_password
```

**Note**: See `env.example` file for a template of all environment variables.

## LTP Scheduler Workflow

A scheduler workflow that automatically runs the LTP Calculator workflow every minute from **9:07 AM to 3:30 PM**.

### Workflow Details

- **Workflow Type**: `LtpSchedulerWorkflow`
- **Task Queue**: `ltpCalculator`
- **Schedule**: Runs every minute from 9:07 AM to 3:30 PM
- **Input Parameters**: Same as `LtpCalculatorWorkflow` (see above)

### Starting LTP Scheduler Workflow from Temporal UI

1. **Open Temporal UI** and navigate to Workflows section
2. **Click "Start Workflow"**
3. **Fill in the workflow details:**
   - **Workflow Type**: `LtpSchedulerWorkflow`
   - **Task Queue**: `ltpCalculator`
   - **Workflow ID**: (optional, recommended: `ltp-scheduler-{indexName}`)
   - **Input**: Provide input as JSON object (same format as LtpCalculatorWorkflow):
     ```json
     {
       "serverName": "MyServer",
       "serverIP": "127.0.0.1",
       "port": "5000",
       "apiKey": "your_api_key_here",
       "indexName": "NIFTY",
       "exchange": "NSE_INDEX",
       "expiry": "27JAN26",
       "strikeRange": 10,
       "scheduleStartTime": "09:07",
       "scheduleEndTime": "15:30",
       "scheduleIntervalMinutes": 1,
       "apiCallPauseMs": 500
     }
     ```
     
     **Schedule Configuration (Optional):**
     - `scheduleStartTime`: Start time in 24-hour format (HH:mm), default: "09:07"
     - `scheduleEndTime`: End time in 24-hour format (HH:mm), default: "15:30"
     - `scheduleIntervalMinutes`: Interval between executions in minutes, default: 1
     - `apiCallPauseMs`: Pause between API calls in milliseconds to avoid throttling, default: 500

4. **Click "Start"** to execute the scheduler

### How It Works

- The scheduler workflow starts and waits until 9:07 AM (if started earlier)
- From 9:07 AM to 3:30 PM, it executes the LTP calculation workflow every minute
- Each execution runs as a child workflow, so failures in one execution don't stop the scheduler
- The scheduler automatically stops at 3:30 PM
- All executions are logged with timestamps and execution counts

### Example Output

```
🕐 Starting LTP Scheduler Workflow
   Schedule: Every minute from 9:07 AM to 3:30 PM
   Server: MyServer
   Index: NIFTY
⏰ [09:07:00] Execution #1
✅ Execution #1 completed: Status: success, Underlying: NIFTY, ATM Strike: 25300
⏰ [09:08:00] Execution #2
✅ Execution #2 completed: Status: success, Underlying: NIFTY, ATM Strike: 25300
...
🛑 Reached end time (3:30 PM). Stopping scheduler.
📊 Scheduler completed. Total executions: 384
```

**Note**: The scheduler runs approximately 384 times (from 9:07 AM to 3:30 PM, once per minute) by default. You can customize the schedule times and interval in the workflow input.

## Docker Compose Deployment

The project includes a `docker-compose.example.yml` file for easy deployment with Docker Compose.

### Setup

1. **Copy the example files:**
   ```bash
   cp env.example .env
   cp docker-compose.example.yml docker-compose.yml
   ```

2. **Edit `.env` file** with your configuration:
   ```bash
   # Temporal Configuration
   TEMPORAL_HOST=192.168.1.112:7233
   
   # Redis Configuration
   REDIS_HOST=redis
   REDIS_PORT=6379
   REDIS_PASSWORD=
   
   # Database Configuration
   DB_HOST=postgres
   DB_PORT=5432
   DB_NAME=pgdb
   DB_USER=pguser
   DB_PASSWORD=pgpass
   
   # LTP Scheduler Configuration
   SCHEDULE_START_TIME=09:07
   SCHEDULE_END_TIME=15:30
   SCHEDULE_INTERVAL_MINUTES=1
   ```

3. **Build and start:**
   ```bash
   docker-compose up -d
   ```

### Schedule Configuration

The schedule times can be configured via environment variables in `.env`:
- `SCHEDULE_START_TIME`: Start time in 24-hour format (HH:mm), default: "09:07"
- `SCHEDULE_END_TIME`: End time in 24-hour format (HH:mm), default: "15:30"
- `SCHEDULE_INTERVAL_MINUTES`: Interval between executions in minutes, default: 1

**Note**: When starting the scheduler workflow from Temporal UI, you can override these defaults by including `scheduleStartTime`, `scheduleEndTime`, and `scheduleIntervalMinutes` in the workflow input JSON.

## NSE Data Download Workflow

A workflow for downloading NSE (National Stock Exchange) data and publishing notifications via Kafka queue with data stored in Redis.

### Workflow Details

- **Single Task Workflow Type**: `DownloadNseDataWorkflow`
- **Batch Workflow Type**: `DownloadNseDataBatchWorkflow`
- **Task Queue**: `downloadNseData`
- **Supported Task Types**: 
  - `allIndices` - Downloads all NSE indices data (CSV format)
  - `optionchange` / `optionchain` - Downloads NSE option chain data (JSON format)
  - `equity` / `equitydata` - Downloads NSE equity data (gainers/losers) (CSV format)

### Starting Single Task Workflow from Temporal UI

1. **Open Temporal UI** and navigate to Workflows section
2. **Click "Start Workflow"**
3. **Fill in the workflow details:**
   - **Workflow Type**: `DownloadNseDataWorkflow`
   - **Task Queue**: `downloadNseData`
   - **Workflow ID**: (optional)
   - **Input**: Provide input as JSON object:
     ```json
     {
       "taskType": "allIndices",
       "taskdelay": 100,
       "taskTimeout": 10000,
       "taskretries": 1
     }
     ```
     
     **Input Parameters:**
     - `taskType` (required): Task type - `"allIndices"`, `"optionchange"`, or `"equity"`
     - `taskdelay` (optional): Delay in milliseconds after each call (default: 0)
     - `taskTimeout` (optional): Timeout in milliseconds for NSE API call (default: 30000)
     - `taskretries` (optional): Number of retries on failure (default: 0, 1 = retry once)

4. **Click "Start"** to execute the workflow

### Starting Batch Workflow from Temporal UI

Execute multiple tasks sequentially in a single workflow to avoid server throttling:

1. **Open Temporal UI** and navigate to Workflows section
2. **Click "Start Workflow"**
3. **Fill in the workflow details:**
   - **Workflow Type**: `DownloadNseDataBatchWorkflow`
   - **Task Queue**: `downloadNseData`
   - **Workflow ID**: (optional)
   - **Input**: Provide input as JSON object:
     ```json
     {
       "tasks": [
         {
           "taskType": "allIndices",
           "taskdelay": 100,
           "taskTimeout": 10000,
           "taskretries": 1
         },
         {
           "taskType": "optionchange",
           "taskdelay": 100,
           "taskTimeout": 10000,
           "taskretries": 1
         }
       ],
       "interTaskDelay": 100
     }
     ```
     
     **Batch Input Parameters:**
     - `tasks` (required): Array of task objects (each with same parameters as single task)
     - `interTaskDelay` (optional): Delay in milliseconds between tasks (default: 0)

4. **Click "Start"** to execute the batch workflow

### Using the Queue System

The workflow publishes notifications to **Kafka** and stores data in **Redis**. Clients can subscribe to Kafka to be notified when new data is available, then read the actual data from Redis.

#### Kafka Queue - Notification System

**Topic**: `nse.data` (common topic for all NSE downloads to avoid race conditions)

**Message Format**:
- **Key**: Redis key where data is stored (e.g., `nse:allindices:current`, `nse:optionchain:current`)
- **Value**: JSON notification with taskName and timestamp:
  ```json
  {
    "taskName": "allIndices",
    "timestamp": "2026-01-28T12:34:56.789Z"
  }
  ```

**Example Kafka Consumer** (Python):
```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'nse.data',
    bootstrap_servers=['localhost:29092', 'localhost:29093', 'localhost:29094'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    notification = message.value
    task_name = notification['taskName']
    timestamp = notification['timestamp']
    redis_key = message.key.decode('utf-8')
    
    print(f"New {task_name} data available at {timestamp}")
    print(f"Redis key: {redis_key}")
    # Read data from Redis using redis_key
```

**Example Kafka Consumer** (Java):
```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:29092,localhost:29093,localhost:29094");
props.put("group.id", "nse-data-consumer");
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Collections.singletonList("nse.data"));

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record : records) {
        String redisKey = record.key();
        String jsonValue = record.value();
        // Parse JSON to get taskName and timestamp
        // Read data from Redis using redisKey
    }
}
```

#### Redis Storage - Data Storage

Data is stored in Redis with separate keys for data and timestamp, with current and previous versions:

**Redis Key Structure**:
- `{taskBase}:current:data` - Latest downloaded data
- `{taskBase}:current:timestamp` - Latest download timestamp
- `{taskBase}:previous:data` - Previous data (rotated from current)
- `{taskBase}:previous:timestamp` - Previous timestamp (rotated from current)

**Task Base Keys**:
- `nse:allindices` - For allIndices task
- `nse:optionchain` - For optionchange/optionchain task
- `nse:equitydata` - For equity/equitydata task

**Example Redis Keys**:
- `nse:allindices:current:data` - Current allIndices CSV data
- `nse:allindices:current:timestamp` - Current allIndices download timestamp
- `nse:allindices:previous:data` - Previous allIndices CSV data
- `nse:allindices:previous:timestamp` - Previous allIndices download timestamp
- `nse:equitydata:current:data` - Current equity data CSV data
- `nse:equitydata:current:timestamp` - Current equity data download timestamp
- `nse:equitydata:previous:data` - Previous equity data CSV data
- `nse:equitydata:previous:timestamp` - Previous equity data download timestamp

**Example Redis Client** (Python):
```python
import redis
import json

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Read current data
data = r.get('nse:allindices:current:data')
timestamp = r.get('nse:allindices:current:timestamp')

print(f"Data downloaded at: {timestamp}")
print(f"Data: {data[:200]}...")  # First 200 chars

# Read previous data
previous_data = r.get('nse:allindices:previous:data')
previous_timestamp = r.get('nse:allindices:previous:timestamp')
```

**Example Redis Client** (Java):
```java
Jedis jedis = new Jedis("localhost", 6379);

// Read current data
String data = jedis.get("nse:allindices:current:data");
String timestamp = jedis.get("nse:allindices:current:timestamp");

System.out.println("Data downloaded at: " + timestamp);
System.out.println("Data: " + data.substring(0, Math.min(200, data.length())));

// Read previous data
String previousData = jedis.get("nse:allindices:previous:data");
String previousTimestamp = jedis.get("nse:allindices:previous:timestamp");
```

### Complete Consumer Flow

1. **Subscribe to Kafka topic** `nse.data`
2. **When notification received**:
   - Parse JSON to get `taskName` and `timestamp`
   - Extract Redis key from Kafka message key
   - Read data from Redis: `{redisKey}:data`
   - Read timestamp from Redis: `{redisKey}:timestamp`
3. **Process the data** as needed

### Configuration

**Kafka Configuration:**
Set the following environment variable to configure Kafka connection:
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default: `localhost:29092,localhost:29093,localhost:29094`)

**Redis Configuration:**
Set the following environment variables to configure Redis connection:
- `REDIS_HOST`: Redis server host (default: `localhost`)
- `REDIS_PORT`: Redis server port (default: `6379`)
- `REDIS_PASSWORD`: Redis password (optional)

**Example Configuration:**
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:29092,localhost:29093,localhost:29094
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_PASSWORD=
```

### Benefits of Common Kafka Topic

- **Avoids Race Conditions**: Single topic ensures ordered message delivery
- **Simplified Client Setup**: Subscribe to one topic instead of multiple
- **Task Filtering**: Filter by `taskName` in message payload if needed
- **Consistent Pattern**: Same structure for all task types

### Data Format

- **allIndices**: Returns CSV format data
- **optionchange**: Returns JSON format data
- **equity**: Returns CSV format data

Both are stored as strings in Redis and can be parsed by consumers based on the `taskName` in the Kafka notification.



