import threading
import time
from typing import Dict, List, Optional
from topic import Topic
from producer import Producer, TaskProducer
from consumer import Consumer, WorkerConsumer

class MessageBroker:
    """
    Central message broker that manages topics, producers, and consumers
    Similar to RabbitMQ or Kafka functionality
    """
    
    def __init__(self, name: str = "MessageBroker"):
        """
        Initialize message broker
        
        Args:
            name: Broker instance name
        """
        self.name = name
        self._topics: Dict[str, Topic] = {}
        self._producers: List[Producer] = []
        self._consumers: List[Consumer] = []
        
        self._topics_lock = threading.Lock()
        self._broker_lock = threading.Lock()
        self._running = False
        self._start_time = None
        
        # Garbage collector thread for expired messages
        self._gc_thread = None
        self._gc_interval = 10.0  # seconds
    
    def create_topic(self, topic_name: str, max_size: Optional[int] = None) -> Topic:
        """
        Create a new topic
        
        Args:
            topic_name: Name of the topic
            max_size: Maximum queue size (None for unbounded)
            
        Returns:
            Topic instance
        """
        with self._topics_lock:
            if topic_name in self._topics:
                print(f"⚠️  Topic '{topic_name}' already exists")
                return self._topics[topic_name]
            
            topic = Topic(topic_name, max_size)
            self._topics[topic_name] = topic
            print(f"📋 Created topic: {topic_name} (max_size={max_size})")
            return topic
    
    def get_topic(self, topic_name: str) -> Optional[Topic]:
        """Get existing topic by name"""
        with self._topics_lock:
            return self._topics.get(topic_name)
    
    def delete_topic(self, topic_name: str) -> bool:
        """Delete a topic"""
        with self._topics_lock:
            if topic_name in self._topics:
                topic = self._topics[topic_name]
                topic.close()
                del self._topics[topic_name]
                print(f"🗑️  Deleted topic: {topic_name}")
                return True
            return False
    
    def add_producer(self, producer: Producer):
        """Add a producer to the broker"""
        with self._broker_lock:
            self._producers.append(producer)
            print(f"➕ Added producer: {producer.producer_id}")
    
    def add_consumer(self, consumer: Consumer):
        """Add a consumer to the broker"""
        with self._broker_lock:
            self._consumers.append(consumer)
            print(f"➕ Added consumer: {consumer.consumer_id}")
    
    def start(self):
        """Start the message broker and all its components"""
        if self._running:
            print("⚠️  Broker already running")
            return
        
        self._running = True
        self._start_time = time.time()
        
        print(f"🚀 Starting Message Broker: {self.name}")
        
        # Start garbage collector
        self._start_garbage_collector()
        
        # Start all producers
        with self._broker_lock:
            for producer in self._producers:
                producer.start()
        
        # Start all consumers
        with self._broker_lock:
            for consumer in self._consumers:
                consumer.start()
        
        print(f"✅ Message Broker {self.name} started with {len(self._topics)} topics, "
              f"{len(self._producers)} producers, {len(self._consumers)} consumers")
    
    def stop(self):
        """Stop the message broker and all its components"""
        if not self._running:
            return
        
        print(f"🛑 Stopping Message Broker: {self.name}")
        self._running = False
        
        # Stop all producers
        with self._broker_lock:
            for producer in self._producers:
                producer.stop()
        
        # Stop all consumers
        with self._broker_lock:
            for consumer in self._consumers:
                consumer.stop()
        
        # Stop garbage collector
        if self._gc_thread:
            self._gc_thread.join(timeout=2.0)
        
        # Close all topics
        with self._topics_lock:
            for topic in self._topics.values():
                topic.close()
        
        print(f"✅ Message Broker {self.name} stopped")
    
    def _start_garbage_collector(self):
        """Start garbage collector thread for expired messages"""
        def gc_worker():
            while self._running:
                try:
                    time.sleep(self._gc_interval)
                    if not self._running:
                        break
                    
                    total_cleaned = 0
                    with self._topics_lock:
                        for topic in self._topics.values():
                            cleaned = topic.cleanup_expired_messages()
                            total_cleaned += cleaned
                    
                    if total_cleaned > 0:
                        print(f"🧹 Garbage Collector: Cleaned {total_cleaned} expired messages")
                        
                except Exception as e:
                    print(f"❌ Garbage collector error: {e}")
        
        self._gc_thread = threading.Thread(target=gc_worker, name="GarbageCollector")
        self._gc_thread.daemon = True
        self._gc_thread.start()
        print("🧹 Garbage collector started")
    
    def get_broker_stats(self) -> dict:
        """Get comprehensive broker statistics"""
        with self._topics_lock, self._broker_lock:
            runtime = time.time() - self._start_time if self._start_time else 0
            
            # Topic statistics
            topic_stats = {}
            total_messages_in_queues = 0
            
            for name, topic in self._topics.items():
                stats = topic.get_stats()
                topic_stats[name] = stats
                total_messages_in_queues += stats['queue_size']
            
            # Producer statistics
            producer_stats = {}
            total_produced = 0
            
            for producer in self._producers:
                stats = producer.get_stats()
                producer_stats[producer.producer_id] = stats
                total_produced += stats['total_produced']
            
            # Consumer statistics
            consumer_stats = {}
            total_consumed = 0
            
            for consumer in self._consumers:
                stats = consumer.get_stats()
                consumer_stats[consumer.consumer_id] = stats
                total_consumed += stats['total_consumed']
            
            return {
                'broker_name': self.name,
                'runtime': runtime,
                'is_running': self._running,
                'topic_count': len(self._topics),
                'producer_count': len(self._producers),
                'consumer_count': len(self._consumers),
                'total_messages_produced': total_produced,
                'total_messages_consumed': total_consumed,
                'messages_in_queues': total_messages_in_queues,
                'throughput_produced_per_sec': total_produced / runtime if runtime > 0 else 0,
                'throughput_consumed_per_sec': total_consumed / runtime if runtime > 0 else 0,
                'topics': topic_stats,
                'producers': producer_stats,
                'consumers': consumer_stats
            }
    
    def print_status(self):
        """Print current broker status"""
        stats = self.get_broker_stats()
        
        print("\n" + "="*60)
        print(f"📊 MESSAGE BROKER STATUS: {stats['broker_name']}")
        print("="*60)
        print(f"Runtime: {stats['runtime']:.1f}s | Running: {stats['is_running']}")
        print(f"Topics: {stats['topic_count']} | Producers: {stats['producer_count']} | Consumers: {stats['consumer_count']}")
        print(f"Total Produced: {stats['total_messages_produced']} | Total Consumed: {stats['total_messages_consumed']}")
        print(f"Messages in Queues: {stats['messages_in_queues']}")
        print(f"Throughput: {stats['throughput_produced_per_sec']:.1f} prod/sec | {stats['throughput_consumed_per_sec']:.1f} cons/sec")
        
        print("\n📋 TOPICS:")
        for topic_name, topic_stats in stats['topics'].items():
            print(f"  • {topic_name}: size={topic_stats['queue_size']}, "
                  f"sent={topic_stats['total_sent']}, consumed={topic_stats['total_consumed']}, "
                  f"consumers={topic_stats['consumer_count']}")
        
        print("\n📤 PRODUCERS:")
        for prod_id, prod_stats in stats['producers'].items():
            print(f"  • {prod_id}: produced={prod_stats['total_produced']}, "
                  f"rate={prod_stats['effective_rate']:.1f}/sec, failed={prod_stats['failed_productions']}")
        
        print("\n📥 CONSUMERS:")
        for cons_id, cons_stats in stats['consumers'].items():
            print(f"  • {cons_id}: consumed={cons_stats['total_consumed']}, "
                  f"rate={cons_stats['consumption_rate']:.1f}/sec, failed={cons_stats['failed_processing']}")
        
        print("="*60)
    
    def wait_for_completion(self, timeout: Optional[float] = None):
        """Wait for all producers and consumers to complete (mainly for testing)"""
        start_time = time.time()
        
        while self._running:
            if timeout and (time.time() - start_time) > timeout:
                break
                
            time.sleep(1.0)
            
            # Check if all queues are empty and no active processing
            all_empty = True
            with self._topics_lock:
                for topic in self._topics.values():
                    if not topic.queue.is_empty():
                        all_empty = False
                        break
            
            if all_empty:
                time.sleep(0.5)  # Give a bit more time
                break