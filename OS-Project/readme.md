# Multi-threaded Message Queue System

A comprehensive implementation of a multi-threaded message queue system in Python, demonstrating key Operating System concepts including thread management, synchronization, and the Producer-Consumer problem.

## 🎯 Project Overview

This project implements a message broker system similar to RabbitMQ or Apache Kafka, featuring:

- **Thread-safe message queues** with proper synchronization
- **Producer-Consumer pattern** implementation
- **Multiple topics** for message organization
- **Bounded and unbounded queues** support
- **Message TTL (Time-To-Live)** functionality
- **Garbage collection** for expired messages
- **Fan-out messaging** (multiple consumers per topic)
- **Real-time statistics** and monitoring

## 🏗️ System Architecture

### Core Components

1. **Message Broker** (`message_broker.py`)
   - Central coordinator managing all components
   - Topic management and routing
   - Statistics collection and monitoring
   - Garbage collection for expired messages

2. **Topic** (`topic.py`)
   - Independent communication channels
   - Thread-safe message queues
   - Consumer registration and fan-out support
   - Statistics tracking

3. **Message Queue** (`message_queue.py`)
   - Thread-safe queue implementation using `collections.deque`
   - Mutex locks and condition variables for synchronization
   - Support for bounded buffers with blocking operations
   - Message expiration handling

4. **Message** (`message.py`)
   - Message structure with ID, content, and metadata
   - TTL (Time-To-Live) support
   - Timestamp tracking

5. **Producer** (`producer.py`)
   - Message generation and publishing
   - Specialized `TaskProducer` for priority-based tasks
   - Configurable production rates
   - Statistics tracking

6. **Consumer** (`consumer.py`)
   - Message consumption and processing
   - Specialized `WorkerConsumer` for task processing
   - Multi-topic subscription support
   - Processing statistics

## 🚀 Installation and Setup

### Requirements
- Python 3.7 or higher
- No external dependencies (uses only Python standard library)

### Installation
```bash
# Clone or download the project files
# Ensure all Python files are in the same directory:
# - main.py
# - message_broker.py
# - topic.py
# - message_queue.py
# - message.py
# - producer.py
# - consumer.py
```

## 🎮 Usage

### Basic Usage
```bash
python main.py <scenario_number> [duration_seconds]
```

### Available Scenarios

#### Scenario 1: Basic Producer-Consumer Demo
```bash
python main.py 1 30
```
- Demonstrates basic message passing
- Multiple producers and consumers
- Shows thread synchronization in action
- Multi-topic consumer example

#### Scenario 2: Task-Based Worker System
```bash
python main.py 2 45
```
- Priority-based task processing
- Worker threads with different capabilities
- Task execution time simulation
- Load balancing demonstration

#### Scenario 3: TTL Messages and Garbage Collection
```bash
python main.py 3 60
```
- Messages with expiration times
- Automatic cleanup of expired messages
- Mixed persistent and ephemeral messaging
- Garbage collector demonstration

### Example Output
```
🚀 Multi-threaded Message Queue System
=====================================
🔧 Setting up Basic Producer-Consumer Demo
📋 Created topic: general_messages (max_size=50)
📋 Created topic: news_updates (max_size=30)
➕ Added producer: Producer-1
➕ Added consumer: Consumer-1
🚀 Starting Message Broker: MainBroker
📤 Producer-1: Published 'Message from Producer-1 at 14:30:15' to 1 topic(s)
📥 Consumer-1: Processing Message(id=abc12345..., content=Message from Producer-1 at 14:30:15)
```

## 🔧 Key Features Implemented

### Thread Synchronization
- **Mutex locks** for thread-safe queue operations
- **Condition variables** (`not_empty`, `not_full`) for efficient blocking
- **Race condition prevention** through proper locking mechanisms

### Producer-Consumer Pattern
- Multiple producers can write to the same topic simultaneously
- Multiple consumers can read from topics concurrently
- Proper handling of queue full/empty conditions
- Fan-out messaging support

### Advanced Features
- **Bounded Buffers**: Configurable maximum queue sizes
- **Message TTL**: Automatic expiration and cleanup
- **Statistics Monitoring**: Real-time performance metrics
- **Graceful Shutdown**: Proper cleanup on termination
- **Signal Handling**: SIGINT/SIGTERM support

### Thread Safety Strategies
- All shared data structures are protected with locks
- Lock acquisition order is consistent to prevent deadlocks
- Condition variables minimize CPU usage during blocking
- Atomic operations for statistics updates

## 📊 Monitoring and Statistics

The system provides comprehensive real-time statistics:

### Broker Level
- Total messages produced/consumed
- Throughput rates (messages/second)
- Queue occupancy levels
- Runtime information

### Topic Level
- Queue sizes and status
- Message flow statistics
- Consumer counts
- Topic-specific metrics

### Producer/Consumer Level
- Individual thread performance
- Success/failure rates
- Processing times
- Task completion statistics

## 🛠️ Synchronization Mechanisms

### Race Condition Prevention
- **Critical Sections**: All queue operations are atomic
- **Mutex Protection**: Shared data structures use locks
- **Condition Variables**: Efficient waiting mechanisms

### Deadlock Avoidance
- Consistent lock ordering
- Timeout mechanisms for critical operations
- Resource cleanup in finally blocks

### Performance Optimization
- Minimal lock holding time
- Efficient data structures (deque for O(1) operations)
- Lazy cleanup of expired messages
- Thread-local statistics where possible

## 🧪 Testing the Implementation

### Stress Testing
```bash
# Run for longer periods to test stability
python main.py 2 300  # 5 minutes

# Monitor system resources
top -p $(pgrep -f "python main.py")
```

### Concurrency Testing
The system automatically tests various concurrency scenarios:
- Multiple producers writing simultaneously
- Multiple consumers reading concurrently
- Queue full/empty conditions
- Message expiration and cleanup

## 📈 Performance Characteristics

### Throughput
- Typical performance: 1000-5000 messages/second (depends on hardware)
- Scales with number of producer/consumer threads
- Bounded by queue sizes and processing times

### Memory Usage
- Efficient memory management with automatic cleanup
- TTL-based garbage collection prevents memory leaks
- Bounded queues limit memory consumption

### Latency
- Low latency message passing (microseconds)
- Condition variables minimize blocking overhead
- Direct memory-to-memory communication

## 🎓 Educational Value

This project demonstrates key OS concepts:

1. **Thread Management**: Creating, starting, and coordinating multiple threads
2. **Synchronization Primitives**: Mutexes, condition variables, atomic operations
3. **Producer-Consumer Problem**: Classic concurrency pattern implementation
4. **Resource Management**: Memory cleanup, thread lifecycle management
5. **System Design**: Modular architecture, separation of concerns
6. **Performance Monitoring**: Statistics collection and analysis

## 🔍 Code Quality Features

- **Type Hints**: Full type annotation for better code clarity
- **Documentation**: Comprehensive docstrings and comments
- **Error Handling**: Robust exception handling throughout
- **Logging**: Detailed status reporting and debugging output
- **Modular Design**: Clean separation of responsibilities
- **Extensibility**: Easy to add new features and components

## 🚨 Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all files are in the same directory
2. **Permission Errors**: Make sure Python has execution permissions
3. **Port Already in Use**: Not applicable (no network components)
4. **High CPU Usage**: Normal for intensive message processing scenarios

### Debug Mode
Add print statements or modify log levels in individual components for detailed debugging.

## 🎯 Project Objectives Achieved

✅ **Thread Management**: Multiple threads created and coordinated  
✅ **Synchronization**: Race conditions prevented with proper locking  
✅ **Producer-Consumer**: Classic problem solved with bounded buffers  
✅ **Thread-Safe Data Structures**: Custom queue implementation  
✅ **Message Broker Functionality**: Topic-based routing system  
✅ **Performance Monitoring**: Real-time statistics and reporting  
✅ **Extensibility**: Multiple scenarios and configurations  

## 📝 License

This project is created for educational purposes as part of an Operating System course. Feel free to use and modify for learning purposes.

## 👨‍💻 Author

Operating System Project Implementation  
Multi-threaded Message Queue System  
Python Implementation - 2024