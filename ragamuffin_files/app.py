import os
import api
import json
import logging
from fastapi import FastAPI
from kafka import KafkaProducer
from contextlib import asynccontextmanager
from ragamuffin_files.aws_uploader import AWSUploader
from ragamuffin_files.aws_lifecycle import AwsLifecycle
from tenacity import retry, wait_fixed, stop_after_delay


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("upload")

@retry(wait=wait_fixed(10), stop=stop_after_delay(120))
def create_kafka_producer():
    """
    Creates and returns a Kafka producer instance.
    
    This function retrieves the Kafka bootstrap servers from the environment variables,
    sets up a Kafka producer with JSON serialization for message values, and returns the producer.
    The function will retry on failure, waiting 5 seconds between attempts, and will stop trying after 60 seconds.
    """
    logger.info("Creating Kafka producer")
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP')
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the lifespan of the FastAPI application.

    This function performs the following tasks during the application's startup and shutdown:
    - Retrieves AWS S3 bucket and path information, and local file path from environment variables.
    - Creates the necessary directories in AWS S3 and locally.
    - Initializes a Kafka producer and starts an asynchronous task to process the message queue.
    - Cleans up resources (local directory, Kafka producer, and message queue) during shutdown.

    Args:
        app (FastAPI): The FastAPI application instance.
    """
    #TODO: Check the type of container connection (gcp, aws, azure)
    producer = create_kafka_producer()
    aws_uploader = AWSUploader(producer)
    aws_lifecycle = AwsLifecycle(aws_uploader)
    aws_lifecycle.on_create()
    yield
    aws_lifecycle.on_destroy()

def create_app() -> FastAPI:
    """
    Create and configure a FastAPI application instance, setting custom 
    paths for the OpenAPI schema and documentation, and including 
    the API router.

    Returns:
        FastAPI: The configured FastAPI application instance.
    """
    app = FastAPI(
        openapi_url="/upload/openapi.json",
        docs_url="/upload/docs",
        lifespan=lifespan
    )
    app.include_router(api.router)
    return app

app = create_app()