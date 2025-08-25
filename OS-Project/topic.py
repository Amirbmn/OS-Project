import threading
import time
from typing import List, Optional
from message import Message
from message_queue import MessageQueue

class Topic:
    """
    Topic represents an independent communication channel
    Each topic has its own message queue with thread-safe operations
    Supports multiple consumers (fan-out pattern)
    """
    
    def __init__(self, name: str, max_size: Optional[int] = None):
        """
        Initialize topic
        
        Args:
            name: Topic name/identifier
            max_size: Maximum queue size (None for unbounded)
        """
        self.name = name
        self.queue = MessageQueue(max_size)
        self._consumers = []  # List of consumer threads
        self._consumer_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        
        # Statistics
        self._total_messages_sent = 0
        self._total_messages_consumed = 0
        self._created_time = time.time()
    
    def publish(self, message: Message, timeout: Optional[float] = None) -> bool:
        """
        Publish message to topic
        
        Args:
            message: Message to publish
            timeout: Timeout for publishing (if queue is full)
            
        Returns:
            True if message was published successfully
        """
        success = self.queue.put(message, timeout)
        if success:
            with self._stats_lock:
                self._total_messages_sent += 1
        return success
    
    def consume(self, timeout: Optional[float] = None) -> Optional[Message]:
        """
        Consume message from topic
        
        Args:
            timeout: Timeout for consuming (if queue is empty)
            
        Returns:
            Message if available, None otherwise
        """
        message = self.queue.get(timeout)
        if message:
            with self._stats_lock:
                self._total_messages_consumed += 1
        return message
    
    def register_consumer(self, consumer_id: str):
        """Register a consumer with this topic"""
        with self._consumer_lock:
            if consumer_id not in self._consumers:
                self._consumers.append(consumer_id)
    
    def unregister_consumer(self, consumer_id: str):
        """Unregister a consumer from this topic"""
        with self._consumer_lock:
            if consumer_id in self._consumers:
                self._consumers.remove(consumer_id)
    
    def get_consumer_count(self) -> int:
        """Get number of registered consumers"""
        with self._consumer_lock:
            return len(self._consumers)
    
    def get_stats(self) -> dict:
        """Get topic statistics"""
        with self._stats_lock:
            return {
                'name': self.name,
                'queue_size': self.queue.size(),
                'is_empty': self.queue.is_empty(),
                'is_full': self.queue.is_full(),
                'total_sent': self._total_messages_sent,
                'total_consumed': self._total_messages_consumed,
                'consumer_count': self.get_consumer_count(),
                'created_time': self._created_time,
                'uptime': time.time() - self._created_time
            }
    
    def cleanup_expired_messages(self) -> int:
        """Clean up expired messages"""
        return self.queue.cleanup_expired_messages()
    
    def close(self):
        """Close the topic"""
        self.queue.close()
    
    def __str__(self):
        stats = self.get_stats()
        return f"Topic(name={self.name}, size={stats['queue_size']}, consumers={stats['consumer_count']})"