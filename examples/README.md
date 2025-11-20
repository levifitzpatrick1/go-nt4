# NT4 Examples

This directory contains example programs demonstrating how to use the go-nt4 library.

## Examples

### 1. Publisher (`publisher/`)
Simple example that publishes various types of data to NetworkTables.

**Usage:**
```bash
cd publisher
go run main.go
```

Publishes:
- `/robot/speed` (double)
- `/robot/mode` (string)
- `/robot/enabled` (boolean)
- `/robot/position` (double array)

### 2. Subscriber (`subscriber/`)
Subscribes to all `/robot/*` topics and prints values periodically.

**Usage:**
```bash
cd subscriber
go run main.go
```

Features:
- Prefix matching subscription
- Batched printing every 2 seconds

### 3. Subscriber with Callback (`subscriber_callback/`)
Subscribes to a specific topic with immediate callback on each update.

**Usage:**
```bash
cd subscriber_callback
go run main.go
```

Features:
- Real-time callback execution
- Demonstrates callback pattern

### 4. Bidirectional (`bidirectional/`)
Both publishes and subscribes, demonstrating two-way communication.

**Usage:**
```bash
cd bidirectional
go run main.go
```

Features:
- Publishes sensor data
- Receives data from other sources
- Connection callbacks

### 5. Team Robot (`team_robot/`)
Example for connecting to a real FRC robot using team number.

**Usage:**
```bash
cd team_robot
go run main.go
```

Features:
- Team number to address conversion
- Topic announcement callbacks
- Robot dashboard display
- Server time synchronization info

## Running Examples

### For Simulation (Local Testing)

All examples default to `127.0.0.1` for simulation. You can test using:
- WPILib's NetworkTables server
- Robot simulation
- Another instance of these examples

### For Real Robot (Team 2064)

Uncomment the robot line in any example:
```go
// Change from:
opts := nt4.DefaultClientOptions("127.0.0.1")

// To:
opts := nt4.DefaultClientOptions(nt4.TeamNumberToAddress(2064))
```

Or for other teams:
```go
opts := nt4.DefaultClientOptions(nt4.TeamNumberToAddress(254)) // Team 254
```

## Testing with Multiple Examples

You can run multiple examples simultaneously to test pub/sub:

**Terminal 1:**
```bash
cd publisher
go run main.go
```

**Terminal 2:**
```bash
cd subscriber
go run main.go
```

**Terminal 3:**
```bash
cd bidirectional
go run main.go
```

## Network Setup

For real robot connections:
1. Connect to the robot's WiFi network
2. Make sure port 5810 is accessible
3. Use the team number address helper: `nt4.TeamNumberToAddress(YOUR_TEAM)`

## Typical Robot Addresses

- **roboRIO:** `10.TE.AM.2` (e.g., Team 2064 = `10.20.64.2`)
- **Simulation:** `127.0.0.1` or `localhost`
- **Custom Server:** Any IP address with NT4 server running
