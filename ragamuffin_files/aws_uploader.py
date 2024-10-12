import os
import logging
from kafka import KafkaProducer
from ragamuffin_files.uploader import Uploader
from ragamuffin_core.aws_rds_helper import RDSHelper
from ragamuffin_core.aws_s3_helper import AwsS3Helper

logger = logging.getLogger(__name__)

class AWSUploader(Uploader):
    """
    AWSUploader is responsible for uploading files to an AWS S3 bucket and saving
    the file information into an AWS RDS database. It extends the `Uploader` class and 
    provides implementations for the `upload` and `save` methods.
    
    Attributes:
        rds_helper (RDSHelper): Helper class used to interact with AWS RDS to store metadata about files.
        bucket (str): The S3 bucket name where files will be uploaded, fetched from the environment variable `AWS_BUCKET_NAME`.
        path (str): The base path in the S3 bucket where files will be stored, fetched from the environment variable `AWS_FILES_PATH`.
    """

    def __init__(self, rds_helper: RDSHelper, producer: KafkaProducer):
        """
        Initializes the AWSUploader with an RDS helper and a Kafka producer.
        
        Args:
            rds_helper (RDSHelper): An instance of the RDSHelper used to interact with AWS RDS.
            producer (KafkaProducer): The Kafka producer used to send messages to Kafka after the upload process.
        """
        super().__init__(producer)
        self.rds_helper = rds_helper
        self.bucket = os.environ.get("AWS_BUCKET_NAME")
        self.path = os.environ.get("AWS_FILES_PATH")

    def upload(self, message):
        """
        Uploads a file to the AWS S3 bucket specified by the `bucket` attribute.

        Args:
            message (dict): The message containing the file metadata. The `file_path` key specifies 
                            the location of the file on the local system, and `file_id` is the file's unique identifier.
        
        Message Format:
            - file_path (str): The local file path to the file being uploaded.
            - file_id (str): A unique identifier for the file.

        Raises:
            Exception: If the file upload to S3 fails, the error is logged along with the file information.
        """
        file_path = message["file_path"]
        file_id = message["file_id"]
        path = f"{self.path}/{os.path.basename(file_path)}"
        try:        
            AwsS3Helper.upload_document(file_path, path, self.bucket)
            logger.info(f"File uploaded: {path}")
        except Exception as error:
            logger.error(f"Error: Could not upload file\n{error}")
            logger.error(f"Failed to upload file: {file_path}, with file_id: {file_id}")

    def save(self, message):
        """
        Saves metadata about the uploaded file into an AWS RDS database.

        Args:
            message (dict): The message containing file metadata. The `file_id`, `user_id`, and `path` keys 
                            represent the file's unique identifier, the user's unique identifier, and the file's path in S3, respectively.
        
        Message Format:
            - file_id (str): A unique identifier for the file.
            - user_id (str): The ID of the user who owns the file.
            - path (str): The path where the file is stored in S3.

        Raises:
            Exception: If saving the file metadata to RDS fails, the error is logged along with the file information.
        """
        try:
            file_id = message["file_id"]
            user_id = message["user_id"]
            path = message["path"]
            saved = self.rds_helper.insert_record(file_id, user_id, path, "uploaded")
            logger.info(f"Saved file: {saved}")
        except Exception as error:
            logger.error(f"Error: Could not save file\n{error}")
            logger.error(f"Failed to save file: {path}, with file_id: {file_id}")