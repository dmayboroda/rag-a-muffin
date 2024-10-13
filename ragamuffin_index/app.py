import os
import shutil
import logging
import asyncio
import nest_asyncio
from indexer import Indexer
from fastapi import FastAPI
from async_loop import loop
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
from ragamuffin_core.common import AsyncQueue
from tenacity import retry, wait_fixed, stop_after_delay

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@retry(wait=wait_fixed(30), stop=stop_after_delay(240))
def create_kafka_consumer():
    """
    Create and return a Kafka consumer instance.

    This function retrieves the Kafka bootstrap servers and topic from the environment variables,
    sets up a Kafka consumer, and returns the consumer instance.
    """
    logger.info("Creating Kafka consumer")
    topic = os.environ.get("KAFKA_TOPIC")
    bootstrap_consuner = os.environ.get("KAFKA_BOOTSTRAP")
    consumer = AIOKafkaConsumer(
        topic,
        loop=asyncio.get_event_loop(),
        consumer_timeout_ms=1000,
        retry_backoff_ms=1000,
        bootstrap_servers=bootstrap_consuner
    )
    return consumer

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the startup and shutdown process for the FastAPI app.
    """
    nest_asyncio.apply()
    consume_task, process_task = init_indexer()
    yield
    consume_task.cancel()
    process_task.cancel()
    shutil.rmtree(os.environ.get("LOCAL_FILES_PATH"))

def init_indexer():
    """
    Initializes and configures the application during startup.

    This function is designed to be called when the application starts. It performs several
    setup tasks, including creating necessary directories, initializing key components like 
    the Kafka consumer, the indexer, and an asynchronous queue. It also starts asynchronous 
    tasks for consuming messages and processing the queue.
    """
    logger.info("Application startup")
    os.makedirs(os.environ.get("LOCAL_FILES_PATH"), exist_ok=True)
    consumer = create_kafka_consumer()
    indexer = Indexer()
    indexer.init_schema()
    index_queue = AsyncQueue()
    consume_task = asyncio.create_task(consume_messages(consumer, index_queue))
    process_task = asyncio.create_task(loop(index_queue, indexer))
    logger.info("Application startup complete")
    return consume_task, process_task

async def consume_messages(consumer, index_queue):
    """
    Asynchronously consumes messages from a Kafka topic and enqueues them for further processing.

    This coroutine starts a Kafka consumer, listens for incoming messages, and enqueues each 
    message into the provided index_queue for further processing. It continuously consumes 
    messages until an interruption or shutdown, ensuring proper resource cleanup when stopped.

    Parameters:
    - consumer: The Kafka consumer instance that listens to a specific topic. 
                It is expected to be an asynchronous consumer (e.g., from `aiokafka`).
    - index_queue: The queue where the consumed messages will be enqueued. 
                   This should support asynchronous enqueueing.
    """
    logger.info("Start Kafka consuming")
    await consumer.start()
    try:
        async for message in consumer:
            logger.info(f"Message: {message.value.decode('utf-8')}")
            index_queue.enqueue(message.value)
    finally:
        await consumer.stop()
        logger.info("Stop Kafka consuming")

def create_app() -> FastAPI:
    """
    Create and configure a FastAPI application instance, setting custom 
    paths for the OpenAPI schema and documentation, and including 
    the API router.

    Returns:
        FastAPI: The configured FastAPI application instance.
    """
    app = FastAPI(
        openapi_url="/index/openapi.json",
        docs_url="/index/docs",
        lifespan=lifespan
    )
    return app

app = create_app()