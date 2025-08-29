import threading
import time
import random
from typing import List, Optional, Callable
from message import Message
from topic import Topic

class Consumer:
    """
    Consumer thread that processes messages from topics
    Runs in a loop, waiting for messages and processing them
    """
    
    def __init__(self, consumer_id: str, topics: List[Topic],
                 message_processor: Optional[Callable] = None):
        """
        Initialize consumer
        
        Args:
            consumer_id: Unique identifier for consumer
            topics: List of topics to consume from
            message_processor: Function to process received messages
        """
        self.consumer_id = consumer_id
        self.topics = topics
        self.message_processor = message_processor or self._default_message_processor
        
        self._running = False
        self._thread = None
        self._total_consumed = 0
        self._failed_processing = 0
        self._stats_lock = threading.Lock()
        self._start_time = None
        
        # Register with topics
        for topic in self.topics:
            topic.register_consumer(self.consumer_id)
    
    def _default_message_processor(self, message: Message) -> bool:
        """Default message processor - just print the message"""
        try:
            print(f"📥 Consumer {self.consumer_id}: Processing {message}")
            # Simulate some processing time
            time.sleep(0.1)
            return True
        except Exception as e:
            print(f" Error processing message in {self.consumer_id}: {e}")
            return False
    
    def start(self):
        """Start the consumer thread"""
        if self._running:
            return
        
        self._running = True
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._run, name=f"Consumer-{self.consumer_id}")
        self._thread.daemon = True
        self._thread.start()
        print(f" Consumer {self.consumer_id} started")
    
    def stop(self):
        """Stop the consumer thread"""
        self._running = False
        
        # Unregister from topics
        for topic in self.topics:
            topic.unregister_consumer(self.consumer_id)
        
        if self._thread:
            self._thread.join(timeout=2.0)
        print(f" Consumer {self.consumer_id} stopped")
    
    def _run(self):
        """Main consumer loop"""
        while self._running:
            try:
                message_found = False
                
                # Try to consume from each topic (round-robin approach)
                for topic in self.topics:
                    message = topic.consume(timeout=0.1)  # Short timeout
                    
                    if message:
                        message_found = True
                        
                        # Check if message is expired
                        if message.is_expired():
                            print(f"  Consumer {self.consumer_id}: Message {message.id[:8]} expired, skipping")
                            continue
                        
                        # Process the message
                        try:
                            success = self.message_processor(message)
                            
                            if success:
                                with self._stats_lock:
                                    self._total_consumed += 1
                            else:
                                with self._stats_lock:
                                    self._failed_processing += 1
                                    
                        except Exception as e:
                            print(f" Error processing message in {self.consumer_id}: {e}")
                            with self._stats_lock:
                                self._failed_processing += 1
                        
                        # Break after processing one message to give other topics a chance
                        break
                
                # If no messages found, sleep briefly
                if not message_found:
                    time.sleep(0.1)
                    
            except Exception as e:
                print(f" Error in consumer {self.consumer_id}: {e}")
                time.sleep(0.1)
    
    def get_stats(self) -> dict:
        """Get consumer statistics"""
        with self._stats_lock:
            runtime = time.time() - self._start_time if self._start_time else 0
            return {
                'consumer_id': self.consumer_id,
                'total_consumed': self._total_consumed,
                'failed_processing': self._failed_processing,
                'runtime': runtime,
                'consumption_rate': self._total_consumed / runtime if runtime > 0 else 0,
                'is_running': self._running,
                'topic_count': len(self.topics)
            }

class WorkerConsumer(Consumer):
    """
    Specialized consumer that processes task-based messages
    Simulates different processing times based on task complexity
    """
    
    def __init__(self, consumer_id: str, topics: List[Topic]):
        super().__init__(consumer_id, topics, self._process_task)
        self._processed_tasks = []
        self._tasks_lock = threading.Lock()
    
    def _process_task(self, message: Message) -> bool:
        """Process a task message"""
        try:
            task_data = message.content
            
            if isinstance(task_data, dict) and 'task_id' in task_data:
                task_id = task_data.get('task_id', 'unknown')
                priority = task_data.get('priority', 5)
                execution_time = task_data.get('execution_time', 0.1)
                description = task_data.get('description', 'No description')
                
                print(f" Worker {self.consumer_id}: Starting task {task_id} (priority={priority}, est_time={execution_time}s)")
                
                # Simulate task execution
                actual_start = time.time()
                time.sleep(execution_time)  # Simulate work
                actual_duration = time.time() - actual_start
                
                # Record completed task
                completed_task = {
                    'task_id': task_id,
                    'priority': priority,
                    'estimated_time': execution_time,
                    'actual_time': actual_duration,
                    'worker_id': self.consumer_id,
                    'completed_at': time.time()
                }
                
                with self._tasks_lock:
                    self._processed_tasks.append(completed_task)
                
                print(f" Worker {self.consumer_id}: Completed task {task_id} in {actual_duration:.2f}s")
                return True
            else:
                # Handle non-task messages with default processor
                return super()._default_message_processor(message)
                
        except Exception as e:
            print(f" Error processing task in worker {self.consumer_id}: {e}")
            return False
    
    def get_completed_tasks(self) -> List[dict]:
        """Get list of completed tasks"""
        with self._tasks_lock:
            return self._processed_tasks.copy()
    
    def get_task_stats(self) -> dict:
        """Get task-specific statistics"""
        with self._tasks_lock:
            if not self._processed_tasks:
                return {'total_tasks': 0}
            
            total_tasks = len(self._processed_tasks)
            avg_time = sum(task['actual_time'] for task in self._processed_tasks) / total_tasks
            
            # Priority distribution
            priority_dist = {}
            for task in self._processed_tasks:
                priority = task['priority']
                priority_dist[priority] = priority_dist.get(priority, 0) + 1
            
            return {
                'total_tasks': total_tasks,
                'average_execution_time': round(avg_time, 2),
                'priority_distribution': priority_dist,
                'latest_task': self._processed_tasks[-1] if self._processed_tasks else None
            }