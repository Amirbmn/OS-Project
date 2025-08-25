#!/usr/bin/env python3
"""
Multi-threaded Message Queue System
Operating System Project Implementation

This program demonstrates:
- Thread management and synchronization
- Producer-Consumer problem solution
- Thread-safe data structures
- Message broker functionality similar to RabbitMQ/Kafka
"""

import time
import threading
import signal
import sys
from typing import List

from message_broker import MessageBroker
from producer import Producer, TaskProducer
from consumer import Consumer, WorkerConsumer
from message import Message

class MessageQueueSystem:
    """Main system orchestrator"""
    
    def __init__(self):
        self.broker = MessageBroker("MainBroker")
        self._shutdown_event = threading.Event()
        self._status_thread = None
        
    def setup_demo_scenario_1(self):
        """Setup basic producer-consumer scenario"""
        print("🔧 Setting up Basic Producer-Consumer Demo")
        
        # Create topics
        general_topic = self.broker.create_topic("general_messages", max_size=50)
        news_topic = self.broker.create_topic("news_updates", max_size=30)
        
        # Create producers
        producer1 = Producer("Producer-1", [general_topic], production_rate=2.0)
        producer2 = Producer("Producer-2", [news_topic], production_rate=1.5)
        
        self.broker.add_producer(producer1)
        self.broker.add_producer(producer2)
        
        # Create consumers
        consumer1 = Consumer("Consumer-1", [general_topic])
        consumer2 = Consumer("Consumer-2", [news_topic])
        consumer3 = Consumer("Consumer-3", [general_topic, news_topic])  # Multi-topic consumer
        
        self.broker.add_consumer(consumer1)
        self.broker.add_consumer(consumer2)
        self.broker.add_consumer(consumer3)
    
    def setup_demo_scenario_2(self):
        """Setup task-based worker scenario with priorities"""
        print("🔧 Setting up Task-Based Worker Demo")
        
        # Create topics for different task types
        urgent_tasks = self.broker.create_topic("urgent_tasks", max_size=20)
        normal_tasks = self.broker.create_topic("normal_tasks", max_size=100)
        background_tasks = self.broker.create_topic("background_tasks")  # Unbounded
        
        # Create task producers
        task_producer1 = TaskProducer("TaskGen-1", [urgent_tasks, normal_tasks], production_rate=1.0)
        task_producer2 = TaskProducer("TaskGen-2", [normal_tasks, background_tasks], production_rate=0.8)
        
        self.broker.add_producer(task_producer1)
        self.broker.add_producer(task_producer2)
        
        # Create worker consumers
        worker1 = WorkerConsumer("Worker-1", [urgent_tasks, normal_tasks])
        worker2 = WorkerConsumer("Worker-2", [normal_tasks, background_tasks])
        worker3 = WorkerConsumer("Worker-3", [urgent_tasks, normal_tasks, background_tasks])
        
        self.broker.add_consumer(worker1)
        self.broker.add_consumer(worker2)
        self.broker.add_consumer(worker3)
    
    def setup_demo_scenario_3(self):
        """Setup mixed scenario with TTL messages"""
        print("🔧 Setting up Mixed Scenario with TTL Messages")
        
        # Create topics
        ephemeral_topic = self.broker.create_topic("ephemeral_messages", max_size=25)
        persistent_topic = self.broker.create_topic("persistent_messages", max_size=50)
        
        # Custom producer for TTL messages
        def ttl_message_generator():
            import random
            ttl_value = random.choice([5.0, 10.0, 15.0, None])  # Various TTL values
            content = f"TTL message (expires in {ttl_value}s)" if ttl_value else "Persistent message"
            return content
        
        # Create producers
        ttl_producer = Producer("TTL-Producer", [ephemeral_topic], 
                               message_generator=ttl_message_generator, production_rate=1.2)
        regular_producer = Producer("Regular-Producer", [persistent_topic], production_rate=0.8)
        
        # Override put method to add TTL
        original_start = ttl_producer.start
        def enhanced_start():
            ttl_producer._original_run = ttl_producer._run
            def ttl_run():
                while ttl_producer._running:
                    try:
                        content = ttl_producer.message_generator()
                        ttl = 8.0 if "TTL message" in content else None
                        message = Message(content, ttl=ttl)
                        
                        if ttl_producer.topics:
                            published_count = 0
                            for topic in ttl_producer.topics:
                                if topic.publish(message, timeout=0.1):
                                    published_count += 1
                                else:
                                    with ttl_producer._stats_lock:
                                        ttl_producer._failed_productions += 1
                            
                            if published_count > 0:
                                with ttl_producer._stats_lock:
                                    ttl_producer._total_produced += published_count
                                print(f"📤 {ttl_producer.producer_id}: Published '{content}' to {published_count} topic(s)")
                        
                        if ttl_producer.delay > 0:
                            time.sleep(ttl_producer.delay)
                    except Exception as e:
                        print(f"❌ Error in producer {ttl_producer.producer_id}: {e}")
                        time.sleep(0.1)
            
            ttl_producer._run = ttl_run
            original_start()
        
        ttl_producer.start = enhanced_start
        
        self.broker.add_producer(ttl_producer)
        self.broker.add_producer(regular_producer)
        
        # Create consumers
        ephemeral_consumer = Consumer("Ephemeral-Consumer", [ephemeral_topic])
        persistent_consumer = Consumer("Persistent-Consumer", [persistent_topic])
        mixed_consumer = Consumer("Mixed-Consumer", [ephemeral_topic, persistent_topic])
        
        self.broker.add_consumer(ephemeral_consumer)
        self.broker.add_consumer(persistent_consumer)
        self.broker.add_consumer(mixed_consumer)
    
    def start_status_monitor(self):
        """Start periodic status reporting"""
        def status_worker():
            while not self._shutdown_event.wait(5.0):  # Report every 5 seconds
                self.broker.print_status()
        
        self._status_thread = threading.Thread(target=status_worker, name="StatusMonitor")
        self._status_thread.daemon = True
        self._status_thread.start()
        print("📊 Status monitor started")
    
    def setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers"""
        def signal_handler(signum, frame):
            print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
            self.shutdown()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def run_scenario(self, scenario_num: int, duration: int = 30):
        """Run a specific demo scenario"""
        print(f"\n🎯 Running Scenario {scenario_num} for {duration} seconds")
        print("="*60)
        
        # Setup scenario
        if scenario_num == 1:
            self.setup_demo_scenario_1()
        elif scenario_num == 2:
            self.setup_demo_scenario_2()
        elif scenario_num == 3:
            self.setup_demo_scenario_3()
        else:
            print("❌ Invalid scenario number")
            return
        
        # Setup signal handlers for graceful shutdown
        self.setup_signal_handlers()
        
        # Start the broker
        self.broker.start()
        
        # Start status monitoring
        self.start_status_monitor()
        
        try:
            # Run for specified duration
            self._shutdown_event.wait(duration)
            
        except KeyboardInterrupt:
            print("\n🛑 Interrupted by user")
        
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Graceful shutdown"""
        print("\n🛑 Initiating graceful shutdown...")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Stop broker
        self.broker.stop()
        
        # Wait for status thread
        if self._status_thread:
            self._status_thread.join(timeout=2.0)
        
        # Print final statistics
        print("\n📊 FINAL STATISTICS:")
        self.broker.print_status()
        
        # Show worker-specific stats for scenario 2
        for consumer in self.broker._consumers:
            if isinstance(consumer, WorkerConsumer):
                task_stats = consumer.get_task_stats()
                if task_stats['total_tasks'] > 0:
                    print(f"\n🔨 {consumer.consumer_id} Task Statistics:")
                    print(f"  Total tasks completed: {task_stats['total_tasks']}")
                    print(f"  Average execution time: {task_stats['average_execution_time']}s")
                    print(f"  Priority distribution: {task_stats['priority_distribution']}")

def main():
    """Main entry point"""
    print("🚀 Multi-threaded Message Queue System")
    print("=====================================")
    
    if len(sys.argv) < 2:
        print("Usage: python main.py <scenario_number> [duration_seconds]")
        print("Scenarios:")
        print("  1 - Basic Producer-Consumer Demo")
        print("  2 - Task-Based Worker Demo with Priorities")
        print("  3 - Mixed Scenario with TTL Messages")
        sys.exit(1)
    
    scenario = int(sys.argv[1])
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    
    system = MessageQueueSystem()
    system.run_scenario(scenario, duration)

if __name__ == "__main__":
    main()


# run in terminal
# python main.py 1 30
  
# python main.py 2 45

# python main.py 3 60


