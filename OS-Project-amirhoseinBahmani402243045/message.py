import time
from typing import Any, Optional
import uuid

class Message: 
    """
    Message structure that contains an ID and content
    Each message has a unique identifier and payload
    """
    
    def __init__(self, content: Any, message_id: Optional[str] = None, ttl: Optional[float] = None):
        """
        Initialize a message
        
        Args:
            content: The message payload (can be string, dict, etc.)
            message_id: Unique identifier for the message (auto-generated if None)
            ttl: Time-to-live in seconds (optional)
        """
        self.id = message_id if message_id else str(uuid.uuid4())
        self.content = content
        self.timestamp = time.time()
        self.ttl = ttl
        self.expiry_time = self.timestamp + ttl if ttl else None
    
    def is_expired(self) -> bool:
        """Check if message has expired based on TTL"""
        if self.expiry_time is None:
            return False
        return time.time() > self.expiry_time
    
    def __str__(self):
        return f"Message(id={self.id[:8]}..., content={self.content}, timestamp={self.timestamp})"
    
    def __repr__(self):
        return self.__str__()