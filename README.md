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
       "strikeRange": 10
     }
     ```
     Or as an array (matching parameter order - first 5 required, last 3 optional with defaults):
     ```json
     ["MyServer", "127.0.0.1", "5000", "your_api_key_here", "NIFTY", "NSE_INDEX", "27JAN26", 10]
     ```
     
     **Note**: `exchange` defaults to "NSE_INDEX", `strikeRange` defaults to 10 if not provided. `expiry` is required.

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
       "strikeRange": 10
     }
     ```

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

**Note**: The scheduler runs approximately 384 times (from 9:07 AM to 3:30 PM, once per minute).
