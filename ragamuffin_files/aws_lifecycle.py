import os
from ragamuffin_core.common import rds_helper
from ragamuffin_files.lifecycle import Lifecycle
import ragamuffin_core.aws_s3_helper as AwsS3Helper

class AwsLifecycle(Lifecycle):
    """
    AwsLifecycle is a subclass of the `Lifecycle` class, which manages the lifecycle of an AWS-based file processing system. 
    It handles the creation and destruction of resources related to AWS S3 and AWS RDS, ensuring the environment is properly
    set up before processing begins and cleaning up when the system shuts down.

    Attributes:
        bucket (str): The AWS S3 bucket name where files will be stored. This is fetched from the environment variable `AWS_BUCKET_NAME`.
        s3_path (str): The path inside the S3 bucket where files will be stored. This is fetched from the environment variable `AWS_FILES_PATH`.
    """

    def __init__(self, uploader):
        """
        Initializes the AwsLifecycle class with the uploader and sets up the AWS-specific configuration.
        
        Args:
            uploader (Uploader): The uploader instance that handles file uploads.
        """
        super().__init__(uploader)
        self.bucket = os.environ.get("AWS_BUCKET_NAME")
        self.s3_path = os.environ.get("AWS_FILES_PATH")

    def on_create(self):
        """
        Lifecycle hook that is invoked when the system is created or started. This method sets up the RDS and S3 environments.

        Steps:
        - Calls the parent class's `on_create` method to handle basic setup.
        - Connects to the RDS database using the `rds_helper`.
        - Creates the necessary RDS tables using the `rds_helper`.
        - Creates the required directory in the S3 bucket using `AwsS3Helper`.

        Raises:
            Exception: If there are errors connecting to RDS or creating the S3 directory, appropriate exceptions are logged.
        """
        super().on_create()
        rds_helper.connect()
        rds_helper.create_table()
        AwsS3Helper.create_directory(self.bucket, self.s3_path)

    def on_destroy(self):
        """
        Lifecycle hook that is invoked when the system is being destroyed or stopped. This method cleans up RDS connections.

        Steps:
        - Calls the parent class's `on_destroy` method to handle basic cleanup.
        - Disconnects from the RDS database using `rds_helper`.

        Raises:
            Exception: If there are errors disconnecting from RDS, appropriate exceptions are logged.
        """
        super().on_destroy()
        rds_helper.disconnect()