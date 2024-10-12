import os
import shutil
import logging
import asyncio
from async_loop import loop
from kafka import KafkaProducer
from abc import ABC, abstractmethod
from ragamuffin_core.common import file_queue
from ragamuffin_files.uploader import Uploader

logger = logging.getLogger(__name__)

class Lifecycle(ABC):
    """
    Abstract base class to manage the lifecycle of a file processing system.
    This class provides lifecycle hooks for resource management, including creating necessary
    directories, initiating asynchronous tasks, and cleaning up resources upon shutdown.
    
    Attributes:
        uploader (Uploader): An instance of the Uploader class responsible for managing file uploads.
        local_path (str): The path to the local directory where files are stored. This is fetched
                          from the 'LOCAL_PATH' environment variable.
        task (asyncio.Task): The task responsible for running the async event loop for processing files
                             from the queue.
    """

    def __init__(self, uploader: Uploader):
       """
        Initializes the Lifecycle with the uploader and sets up the local file path and task.
        
        Args:
            uploader (Uploader): The uploader instance that handles file uploads.
       """
       self.uploader = uploader
       self.local_path = os.environ.get("LOCAL_PATH")
       self.task  = None

    @abstractmethod
    def on_create(self):
        """
        Lifecycle hook that is invoked when the system is created or started. This method ensures 
        that the local directory is created, and it starts the async task to process files from the queue.
        
        Steps:
        - Ensures the local directory (defined by `local_path`) exists or creates it.
        - Starts an asynchronous task to run the event loop that processes the file queue using 
          the provided uploader.

        Raises:
            OSError: If there is an issue creating the local directory.
        """
        os.makedirs(self.local_path, exist_ok=True)
        self.task = asyncio.create_task(loop(file_queue, self.producer, self.uploader))

    @abstractmethod
    def on_destroy(self):
        """
        Lifecycle hook that is invoked when the system is being destroyed or stopped. 
        This method cleans up resources, cancels the async task, and shuts down the file queue.
        
        Steps:
        - Removes the local directory (defined by `local_path`).
        - Flushes and closes the Kafka producer to ensure all messages are sent.
        - Cancels the running async task.
        - Shuts down the file queue to stop further processing.
        
        Raises:
            OSError: If there is an issue removing the local directory.
        """
        shutil.rmtree(self.local_path)
        self.uploader.producer.flush()
        self.uploader.producer.close()
        self.task.cancel()
        file_queue.shutdown()