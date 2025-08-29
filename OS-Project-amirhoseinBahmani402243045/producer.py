import threading
import time
import random
from typing import List, Optional, Callable
from message import Message
from topic import Topic

class Producer:
    """
    Producer thread that generates and publishes messages to topics
    Runs in a loop, creating messages and adding them to specified topics
    """
    
    def __init__(self, producer_id: str, topics: List[Topic], 
                 message_generator: Optional[Callable] = None,
                 production_rate: float = 1.0):
       
        self.producer_id = producer_id
        self.topics = topics
        self.message_generator = message_generator or self._default_message_generator
        self.production_rate = production_rate
        self.delay = 1.0 / production_rate if production_rate > 0 else 0
        
        self._running = False
        self._thread = None
        self._total_produced = 0
        self._failed_productions = 0
        self._stats_lock = threading.Lock()
        self._start_time = None
    
    def _default_message_generator(self) -> str:
  
        return f"Message from {self.producer_id} at {time.strftime('%H:%M:%S')}"
    
    def start(self):
      
        if self._running:
            return
        
        self._running = True
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._run, name=f"Producer-{self.producer_id}")
        self._thread.daemon = True
        self._thread.start()
        print(f" Producer {self.producer_id} started")
    
    def stop(self):
       
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
        print(f" Producer {self.producer_id} stopped")
    
    def _run(self):
        """Main producer loop"""
        while self._running:
            try:
                # Generate message content
                content = self.message_generator()
                message = Message(content)
                
                # Select topic(s) to publish to
                if self.topics:
                    
                   
                    published_count = 0
                    
                    for topic in self.topics:
                        # Try to publish with a short timeout
                        if topic.publish(message, timeout=0.1):
                            published_count += 1
                        else:
                            with self._stats_lock:
                                self._failed_productions += 1
                    
                    if published_count > 0:
                        with self._stats_lock:
                            self._total_produced += published_count
                        
                        print(f" Producer {self.producer_id}: Published '{content}' to {published_count} topic(s)")
                
                # Wait before next message
                if self.delay > 0:
                    time.sleep(self.delay)
                    
            except Exception as e:
                print(f" Error in producer {self.producer_id}: {e}")
                with self._stats_lock:
                    self._failed_productions += 1
                time.sleep(0.1)  # Brief pause on error
    
    def get_stats(self) -> dict:
        """Get producer statistics"""
        with self._stats_lock:
            runtime = time.time() - self._start_time if self._start_time else 0
            return {
                'producer_id': self.producer_id,
                'total_produced': self._total_produced,
                'failed_productions': self._failed_productions,
                'production_rate': self.production_rate,
                'runtime': runtime,
                'effective_rate': self._total_produced / runtime if runtime > 0 else 0,
                'is_running': self._running,
                'topic_count': len(self.topics)
            }

class TaskProducer(Producer):
    """
    Specialized producer for task-based messages with priorities and execution times
    """
    
    def __init__(self, producer_id: str, topics: List[Topic], 
                 production_rate: float = 1.0):
        super().__init__(producer_id, topics, self._generate_task, production_rate)
        self.task_counter = 0
    
    def _generate_task(self) -> dict:
        """Generate a task message with priority and estimated execution time"""
        self.task_counter += 1
        
        # Random priority (1=high, 5=low)
        priority = random.randint(1, 5)
        
        # Random execution time (0.1 to 3.0 seconds)
        execution_time = round(random.uniform(0.1, 3.0), 2)
        
        task = {
            'task_id': f"{self.producer_id}-task-{self.task_counter}",
            'priority': priority,
            'execution_time': execution_time,
            'description': f"Task {self.task_counter} from {self.producer_id}",
            'created_at': time.time()
        }
        
        return task