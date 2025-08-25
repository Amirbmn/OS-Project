import threading
import time
from collections import deque
from typing import Optional, List
from message import Message

class MessageQueue:
    """
    Thread-safe message queue implementation using linked list (deque)
    Supports bounded buffer and message expiration
    """
    
    def __init__(self, max_size: Optional[int] = None):
        """
        Initialize message queue
        
        Args:
            max_size: Maximum queue capacity (None for unbounded)
        """
        self._queue = deque()
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock) if max_size else None
        self._max_size = max_size
        self._closed = False
    
    def put(self, message: Message, timeout: Optional[float] = None) -> bool:
        """
        Add message to queue
        
        Args:
            message: Message to add
            timeout: Maximum time to wait if queue is full
            
        Returns:
            True if message was added, False if timeout or queue closed
        """
        end_time = time.time() + timeout if timeout else None
        
        with self._lock:
            # Wait if queue is full (bounded buffer)
            while (self._max_size and len(self._queue) >= self._max_size and 
                   not self._closed):
                if not self._not_full:
                    return False  # Unbounded queue, should not reach here
                
                if timeout is not None:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        return False
                    if not self._not_full.wait(remaining):
                        return False
                else:
                    self._not_full.wait()
            
            if self._closed:
                return False
            
            # Add message to end of queue
            self._queue.append(message)
            
            # Notify consumers that queue is not empty
            self._not_empty.notify()
            
            return True
    
    def get(self, timeout: Optional[float] = None) -> Optional[Message]:
        """
        Get message from queue (blocks if empty)
        
        Args:
            timeout: Maximum time to wait for message
            
        Returns:
            Message if available, None if timeout or queue closed
        """
        end_time = time.time() + timeout if timeout else None
        
        with self._lock:
            # Wait while queue is empty
            while len(self._queue) == 0 and not self._closed:
                if timeout is not None:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        return None
                    if not self._not_empty.wait(remaining):
                        return None
                else:
                    self._not_empty.wait()
            
            if self._closed and len(self._queue) == 0:
                return None
            
            # Get message from front of queue (FIFO)
            message = self._queue.popleft()
            
            # Notify producers that queue is not full
            if self._not_full:
                self._not_full.notify()
            
            return message
    
    def size(self) -> int:
        """Get current queue size"""
        with self._lock:
            return len(self._queue)
    
    def is_empty(self) -> bool:
        """Check if queue is empty"""
        with self._lock:
            return len(self._queue) == 0
    
    def is_full(self) -> bool:
        """Check if queue is full (for bounded queues)"""
        with self._lock:
            return self._max_size and len(self._queue) >= self._max_size
    
    def close(self):
        """Close the queue and wake up all waiting threads"""
        with self._lock:
            self._closed = True
            self._not_empty.notify_all()
            if self._not_full:
                self._not_full.notify_all()
    
    def cleanup_expired_messages(self) -> int:
        """Remove expired messages and return count of removed messages"""
        removed_count = 0
        with self._lock:
            # Create new deque with non-expired messages
            new_queue = deque()
            while self._queue:
                message = self._queue.popleft()
                if not message.is_expired():
                    new_queue.append(message)
                else:
                    removed_count += 1
            
            self._queue = new_queue
            
            # Notify producers if space became available
            if self._not_full and removed_count > 0:
                self._not_full.notify_all()
        
        return removed_count