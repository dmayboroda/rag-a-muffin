import asyncio
import logging
from ragamuffin_files.uploader import Uploader
from ragamuffin_core.common import AsyncQueue
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)
executor = ThreadPoolExecutor()

async def loop(async_queue: AsyncQueue, uploader: Uploader):
    """
    Continuously process files from the async_queue, upload them to AWS S3, 
    and send a message to a Kafka topic. If the queue is empty, it sleeps for a short 
    period before checking again.

    Args:
        async_queue (AsyncQueue): The queue to dequeue file paths from.
        logger (Logger): The logger to log information and errors.
        producer (KafkaProducer): The Kafka producer to send messages to the specified topic.

    Raises:
        Exception: If an error occurs during the file processing, uploading, or Kafka message sending.
    """
    while True:
        if async_queue.size() == 0:
            await asyncio.sleep(0.1) 
            continue
        try:
            message = await async_queue.dequeue()
            file_id = message["file_id"]
            user_id = message["user_id"]
            file_path = message["file_path"]
            logger.info(f"User with id: {user_id} uploaded file with id: {file_id}, path: {file_path}")
            loop = asyncio.get_running_loop()
            message = {
                "user_id": user_id,
                "file_id": file_id,
                "file_path": file_path,
            }
            loop.run_in_executor(executor, uploader.process_message, message)
        except Exception as e:
            logger.error(f"Error in loop: {e}")
            logger.error(f"Failed to upload file: {file_path}")