import os
import logging
from kafka import KafkaProducer
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class Uploader(ABC):
    """
    Abstract base class for an uploader that processes and sends messages to Kafka.
    Subclasses must implement the `upload` method to define specific upload behavior.
    
    Attributes:
        producer (KafkaProducer): The Kafka producer used to send messages to a Kafka topic.
        topic (str): The Kafka topic to which messages are sent. This is fetched from the 'KAFKA_TOPIC'
                     environment variable.
    """

    def __init__(self, producer: KafkaProducer):
       """
        Initialize the Uploader with a KafkaProducer instance and set the Kafka topic.

        Args:
            producer (KafkaProducer): The Kafka producer instance used to send messages.
        """
       self.producer = producer
       self.topic = os.environ.get("KAFKA_TOPIC")

    @abstractmethod
    def upload(self, message):
        """
        Abstract method to handle the upload of a message. This must be implemented by any subclass
        of the `Uploader` class.

        Args:
            message (dict): The message data to be uploaded. The structure of the message
                            depends on the specific implementation of the subclass.
        """
        pass

    @abstractmethod
    def save(self, message):
        """
        Optional method to save the message after uploading. This can be overridden by subclasses 
        to define custom save behavior if needed.

        Args:
            message (dict): The message data to be saved. 
        """
        pass

    def process_message(self, message):
        """
        Process a given message by uploading it, saving it, sending it to the Kafka topic, 
        and removing the corresponding file. This method handles the message end-to-end 
        with error handling.

        Args:
            message (dict): The message to be processed. It should contain a 'file_path' key,
                            which points to the location of the file that will be removed after
                            processing.

        Raises:
            Exception: Catches and logs any error that occurs during processing (upload, save, Kafka send, or file removal).
        """
        try:
            self.upload(message)
            self.save(message)
            self.producer.send(self.topic, message)
            os.remove(message["file_path"])
        except Exception as error:
            logger.error(f"Error: Could not upload file\n{error}")

