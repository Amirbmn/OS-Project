#!/usr/bin/env python3
"""
Test Examples and Demonstrations for Message Queue System
Shows various usage patterns and edge cases
"""

import time
import threading
from message_broker import MessageBroker
from producer import Producer, TaskProducer
from consumer import Consumer, WorkerConsumer
from message import Message

def test_basic_functionality():
    """Test basic message queue functionality"""
    print("🧪 Testing Basic Functionality")
    
    broker = MessageBroker("TestBroker")
    topic = broker.create_topic("test_topic", max_size=10)
    
    # Test message creation and TTL
    msg1 = Message("Test message 1")
    msg2 = Message("Test message 2", ttl=1.0)  # Expires in 1 second
    
    print(f"Created messages: {msg1}, {msg2}")
    
    # Test queue operations
    topic.publish(msg1)
    topic.publish(msg2)
    
    print(f"Topic stats after publishing: {topic.get_stats()}")
    
    # Consume immediately
    consumed1 = topic.consume()
    print(f"Consumed: {consumed1}")
    
    # Wait for TTL expiration
    time.sleep(1.5)
    
    # Clean up expired messages
    cleaned = topic.cleanup_expired_messages()
    print(f"Cleaned {cleaned} expired messages")
    
    # Try to consume expired message
    consumed2 = topic.consume(timeout=0.1)
    print(f"Consumed after expiration: {consumed2}")
    
    broker.stop()
    print("✅ Basic functionality test completed\n")

def test_bounded_buffer():
    """Test bounded buffer behavior"""
    print("🧪 Testing Bounded Buffer")
    
    broker = MessageBroker("BoundedTestBroker")
    small_topic = broker.create_topic("small_topic", max_size=3)
    
    # Fill the buffer
    for i in range(3):
        msg = Message(f"Message {i+1}")
        result = small_topic.publish(msg)
        print(f"Published message {i+1}: {result}")
    
    print(f"Topic full: {small_topic.queue.is_full()}")
    
    # Try to add one more (should fail with timeout)
    overflow_msg = Message("Overflow message")
    result = small_topic.publish(overflow_msg, timeout=0.1)
    print(f"Overflow publish result: {result}")
    
    # Consume one to make space
    consumed = small_topic.consume()
    print(f"Consumed to make space: {consumed}")
    
    # Now overflow message should succeed
    result = small_topic.publish(overflow_msg, timeout=0.1)
    print(f"Retry overflow publish: {result}")
    
    broker.stop()
    print("✅ Bounded buffer test completed\n")

def test_multiple_consumers_same_topic():
    """Test fan-out messaging with multiple consumers"""
    print("🧪 Testing Fan-out Messaging")
    
    broker = MessageBroker("FanoutBroker")
    shared_topic = broker.create_topic("shared_topic")
    
    # Create multiple consumers for same topic
    consumers = []
    for i in range(3):
        consumer = Consumer(f"Consumer-{i+1}", [shared_topic])
        consumers.append(consumer)
        broker.add_consumer(consumer)
    
    # Create producer
    producer = Producer("TestProducer", [shared_topic], production_rate=2.0)
    broker.add_producer(producer)
    
    # Start system
    broker.start()
    
    # Run for a short time
    time.sleep(5)
    
    # Print statistics
    broker.print_status()
    
    broker.stop()
    print("✅ Fan-out messaging test completed\n")

def test_priority_task_processing():
    """Test task processing with different priorities"""
    print("🧪 Testing Priority Task Processing")
    
    broker = MessageBroker("PriorityBroker")
    
    # Create topics for different priorities
    high_priority = broker.create_topic("high_priority", max_size=10)
    normal_priority = broker.create_topic("normal_priority", max_size=20)
    low_priority = broker.create_topic("low_priority")
    
    # Create specialized workers
    urgent_worker = WorkerConsumer("UrgentWorker", [high_priority])
    general_worker = WorkerConsumer("GeneralWorker", [high_priority, normal_priority])
    background_worker = WorkerConsumer("BackgroundWorker", [normal_priority, low_priority])
    
    broker.add_consumer(urgent_worker)
    broker.add_consumer(general_worker)
    broker.add_consumer(background_worker)
    
    # Manually create some priority tasks
    tasks = [
        {"task_id": "URGENT-001", "priority": 1, "execution_time": 0.5, "description": "Critical system task"},
        {"task_id": "NORMAL-001", "priority": 3, "execution_time": 1.0, "description": "Regular processing task"},
        {"task_id": "LOW-001", "priority": 5, "execution_time": 2.0, "description": "Background cleanup task"},
        {"task_id": "URGENT-002", "priority": 1, "execution_time": 0.3, "description": "Emergency response"},
    ]
    
    broker.start()
    
    # Publish tasks to appropriate topics
    for task in tasks:
        message = Message(task)
        if task["priority"] == 1:
            high_priority.publish(message)
            print(f"📤 Published urgent task: {task['task_id']}")
        elif task["priority"] <= 3:
            normal_priority.publish(message)
            print(f"📤 Published normal task: {task['task_id']}")
        else:
            low_priority.publish(message)
            print(f"📤 Published low priority task: {task['task_id']}")
    
    # Wait for processing
    time.sleep(8)
    
    # Show worker statistics
    for consumer in [urgent_worker, general_worker, background_worker]:
        task_stats = consumer.get_task_stats()
        print(f"\n🔨 {consumer.consumer_id} completed {task_stats.get('total_tasks', 0)} tasks")
        if task_stats.get('total_tasks', 0) > 0:
            print(f"   Average execution time: {task_stats.get('average_execution_time', 0)}s")
            print(f"   Completed tasks: {[task['task_id'] for task in consumer.get_completed_tasks()]}")
    
    broker.stop()
    print("✅ Priority task processing test completed\n")

def test_message_expiration():
    """Test message TTL and garbage collection"""
    print("🧪 Testing Message Expiration")
    
    broker = MessageBroker("TTLBroker")
    topic = broker.create_topic("ttl_topic")
    
    # Create messages with different TTL values
    messages = [
        Message("Short-lived message", ttl=1.0),
        Message("Medium-lived message", ttl=3.0),
        Message("Long-lived message", ttl=5.0),
        Message("Persistent message"),  # No TTL
    ]
    
    # Publish all messages
    for msg in messages:
        topic.publish(msg)
        print(f"📤 Published: {msg}")
    
    print(f"Initial topic size: {topic.queue.size()}")
    
    # Wait and check at intervals
    for i in range(6):
        time.sleep(1)
        cleaned = topic.cleanup_expired_messages()
        remaining = topic.queue.size()
        print(f"After {i+1}s: cleaned {cleaned} messages, {remaining} remaining")
    
    # Check what's left
    remaining_msg = topic.consume(timeout=0.1)
    if remaining_msg:
        print(f"Remaining message: {remaining_msg}")
    
    broker.stop()
    print("✅ Message expiration test completed\n")

def test_concurrent_access():
    """Test concurrent access patterns"""
    print("🧪 Testing Concurrent Access")
    
    broker = MessageBroker("ConcurrentBroker")
    topic = broker.create_topic("concurrent_topic", max_size=50)
    
    # Statistics tracking
    stats = {
        'published': 0,
        'consumed': 0,
        'lock': threading.Lock()
    }
    
    def producer_worker(worker_id, count):
        """Producer worker function"""
        for i in range(count):
            msg = Message(f"Message {i} from Producer {worker_id}")
            if topic.publish(msg, timeout=1.0):
                with stats['lock']:
                    stats['published'] += 1
            time.sleep(0.01)  # Small delay
    
    def consumer_worker(worker_id, duration):
        """Consumer worker function"""
        end_time = time.time() + duration
        while time.time() < end_time:
            msg = topic.consume(timeout=0.1)
            if msg:
                with stats['lock']:
                    stats['consumed'] += 1
                time.sleep(0.005)  # Processing time
    
    # Create multiple producer threads
    producer_threads = []
    for i in range(3):
        thread = threading.Thread(target=producer_worker, args=(i+1, 20))
        producer_threads.append(thread)
    
    # Create multiple consumer threads
    consumer_threads = []
    for i in range(2):
        thread = threading.Thread(target=consumer_worker, args=(i+1, 5.0))
        consumer_threads.append(thread)
    
    # Start all threads
    print("Starting concurrent producers and consumers...")
    
    for thread in producer_threads + consumer_threads:
        thread.start()
    
    # Wait for all threads to complete
    for thread in producer_threads + consumer_threads:
        thread.join()
    
    print(f"Final statistics: Published {stats['published']}, Consumed {stats['consumed']}")
    print(f"Messages remaining in queue: {topic.queue.size()}")
    
    broker.stop()
    print("✅ Concurrent access test completed\n")

def test_error_handling():
    """Test error handling and edge cases"""
    print("🧪 Testing Error Handling")
    
    broker = MessageBroker("ErrorBroker")
    topic = broker.create_topic("error_topic", max_size=5)
    
    # Test consuming from empty queue with timeout
    print("Testing empty queue consumption...")
    start_time = time.time()
    msg = topic.consume(timeout=0.5)
    elapsed = time.time() - start_time
    print(f"Empty queue consume took {elapsed:.2f}s, result: {msg}")
    
    # Test publishing to closed topic
    print("Testing closed topic operations...")
    topic.close()
    
    test_msg = Message("Test message for closed topic")
    result = topic.publish(test_msg, timeout=0.1)
    print(f"Publish to closed topic result: {result}")
    
    msg = topic.consume(timeout=0.1)
    print(f"Consume from closed topic result: {msg}")
    
    # Test with malformed messages
    print("Testing error recovery...")
    error_topic = broker.create_topic("error_recovery_topic")
    
    class ErrorConsumer(Consumer):
        def __init__(self, consumer_id, topics):
            super().__init__(consumer_id, topics, self.error_processor)
            self.error_count = 0
        
        def error_processor(self, message):
            # Simulate processing errors
            if "error" in str(message.content).lower():
                self.error_count += 1
                raise Exception(f"Simulated processing error #{self.error_count}")
            return True
    
    error_consumer = ErrorConsumer("ErrorConsumer", [error_topic])
    broker.add_consumer(error_consumer)
    
    # Add messages that will cause errors
    test_messages = [
        Message("Normal message 1"),
        Message("ERROR message - this will fail"),
        Message("Normal message 2"),
        Message("Another ERROR message"),
        Message("Normal message 3")
    ]
    
    broker.start()
    
    for msg in test_messages:
        error_topic.publish(msg)
        time.sleep(0.1)
    
    time.sleep(2)  # Let consumer process
    
    stats = error_consumer.get_stats()
    print(f"Error consumer stats: {stats['total_consumed']} consumed, {stats['failed_processing']} failed")
    
    broker.stop()
    print("✅ Error handling test completed\n")

def run_all_tests():
    """Run all test cases"""
    print("🚀 Running All Test Cases")
    print("=" * 50)
    
    test_functions = [
        test_basic_functionality,
        test_bounded_buffer,
        test_multiple_consumers_same_topic,
        test_priority_task_processing,
        test_message_expiration,
        test_concurrent_access,
        test_error_handling
    ]
    
    for test_func in test_functions:
        try:
            test_func()
        except Exception as e:
            print(f"❌ Test {test_func.__name__} failed: {e}")
        
        time.sleep(1)  # Brief pause between tests
    
    print("🎉 All tests completed!")

def performance_benchmark():
    """Simple performance benchmark"""
    print("🚀 Performance Benchmark")
    print("=" * 30)
    
    broker = MessageBroker("BenchmarkBroker")
    topic = broker.create_topic("benchmark_topic", max_size=1000)
    
    # High-speed producer
    producer = Producer("BenchmarkProducer", [topic], production_rate=100.0)  # 100 msg/sec
    
    # Multiple consumers
    consumers = []
    for i in range(3):
        consumer = Consumer(f"BenchmarkConsumer-{i+1}", [topic])
        consumers.append(consumer)
        broker.add_consumer(consumer)
    
    broker.add_producer(producer)
    
    print("Starting benchmark - 10 seconds of high-throughput messaging...")
    
    start_time = time.time()
    broker.start()
    
    # Run benchmark
    time.sleep(10)
    
    # Final stats
    broker.print_status()
    
    runtime = time.time() - start_time
    stats = broker.get_broker_stats()
    
    print(f"\n📊 Benchmark Results:")
    print(f"Runtime: {runtime:.1f}s")
    print(f"Messages produced: {stats['total_messages_produced']}")
    print(f"Messages consumed: {stats['total_messages_consumed']}")
    print(f"Production rate: {stats['total_messages_produced']/runtime:.1f} msg/sec")
    print(f"Consumption rate: {stats['total_messages_consumed']/runtime:.1f} msg/sec")
    
    broker.stop()
    print("✅ Performance benchmark completed\n")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        
        test_map = {
            "basic": test_basic_functionality,
            "bounded": test_bounded_buffer,
            "fanout": test_multiple_consumers_same_topic,
            "priority": test_priority_task_processing,
            "ttl": test_message_expiration,
            "concurrent": test_concurrent_access,
            "errors": test_error_handling,
            "benchmark": performance_benchmark,
            "all": run_all_tests
        }
        
        if test_name in test_map:
            test_map[test_name]()
        else:
            print(f"Unknown test: {test_name}")
            print(f"Available tests: {', '.join(test_map.keys())}")
    else:
        print("Usage: python test_examples.py <test_name>")
        print("Available tests:")
        print("  basic      - Basic functionality")
        print("  bounded    - Bounded buffer behavior")
        print("  fanout     - Fan-out messaging")
        print("  priority   - Priority task processing")
        print("  ttl        - Message expiration")
        print("  concurrent - Concurrent access patterns")
        print("  errors     - Error handling")
        print("  benchmark  - Performance benchmark")
        print("  all        - Run all tests")

# run in terminal
# python test_examples.py basic
# python test_examples.py benchmark
# python test_examples.py all