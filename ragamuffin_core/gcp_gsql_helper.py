import os
import json
import logging
from google.oauth2 import service_account

import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes
import pg8000

logger = logging.getLogger(__name__)

class GSQLHelper:
    def __init__(self, config):
        """
        Initialize the GSQLHelper with configuration parameters.
        
        Args:
            config (dict): Configuration dictionary containing SQL queries.
        """
        self.host = os.environ.get("GSQL_DB_HOST")
        self.db_instance = os.environ.get("GSQL_DB_INSTANCE")
        self.database = os.environ.get("GSQL_DB_NAME")
        self.user = os.environ.get("GSQL_DB_USER")
        self.password = os.environ.get("GSQL_DB_PASSWORD")
        self.port = os.environ.get("GSQL_DB_PORT")
        self.connection = None
        self.gsql_config = config['gsql']

    def disconnect(self):
        """
        Close the database cursor and connection.
        """
        
        pass
        logger.info("Disconnected from the database")

    def create_table(self):
        """
        Create a table in the database using the query from the configuration.
        """
        
        try:
            with self.connection.connect() as conn:
                create_table_query = self.gsql_config['create_table']
                conn.execute(sqlalchemy.text(create_table_query))
                conn.commit()
                logger.info("Table created successfully in GCP")
        except Exception as error:
            logger.error(f"Error: Could not create table\n{error}")

    def insert_record(self, file_id, user_id, file_name, status):
        """
        Insert a record into the database.

        Args:
            file_id (str): ID of the file.
            user_id (str): ID of the user.
            file_name (str): Name of the file.
            status (str): Status of the file.
        Returns:
            str: JSON string containing the inserted record details or error message.
        """
        try:
            with self.connection.connect() as conn:
                insert_query = self.gsql_config['insert_record']
                conn.execute(sqlalchemy.text(insert_query), {'file_id': file_id, 'user_id': user_id, 'file_name': file_name, 'status': status})
                conn.commit()
                record = conn.execute(sqlalchemy.text(self.gsql_config['select_inserted_record']), {'file_id': file_id, 'user_id': user_id, 'file_name': file_name, 'status': status}).fetchone()
                logger.info(f"Records inserted successfully, number of records: {len(record)}")
                return json.dumps({
                    "id": record[0],
                    "file_id": record[1],
                    "user_id": record[2],
                    "file_name": record[3],
                    "status": record[4]
                })
        except Exception as error:
            logger.error(f"Error: Could not insert record\n{error}")
            self.connection.rollback()
            return json.dumps({"error": str(error)})

    def fetch_records_by_user_id(self, user_id):
        """
        Fetch records from the database by user ID.

        Args:
            user_id (str): ID of the user.

        Returns:
            str: JSON string containing the fetched records or error message.
        """
        try:
            with self.connection.connect() as conn:
                fetch_query = sqlalchemy.text(self.gsql_config['records_by_user_id'])
                conn.execute(fetch_query, {'user_id': user_id})
                records = conn.fetchall()
                logger.info(f"Fetched {len(records)} records, user_id: {user_id}")
                return json.dumps([{
                    "id": record[0],
                    "file_id": record[1],
                    "user_id": record[2],
                    "file_name": record[3],
                    "status": record[4]
                } for record in records])
        except Exception as error:
            logger.error(f"Error: Could not fetch records\n{error}")
            return json.dumps({"error": str(error)})
    
    def update_status_for_files(self, file_ids, new_status):
        """
        Update the status for multiple files.

        Args:
            file_ids (list): List of file IDs to update.
            new_status (str): New status to set for the files.

        Returns:
            str: JSON string containing the updated records or error message.
        """
        try:
            with self.connection.connect() as conn:
                update_query = sqlalchemy.text(self.gsql_config['update_files_status'])
                conn.execute(update_query, {'status': new_status, 'file_ids': file_ids})
                conn.commit()
                select_updated_records = sqlalchemy.text(self.gsql_config['select_updated_records'])
                updated_records = conn.execute(select_updated_records, {'status': new_status, 'file_ids': file_ids}).fetchall()
                logger.info(f"Updated {len(updated_records)} records")
                return json.dumps([{
                    "id": record[0],
                    "file_id": record[1],
                    "user_id": record[2],
                    "file_name": record[3],
                    "status": record[4],
                } for record in updated_records])
        except Exception as error:
            logger.error(f"Error: Could not update records\n{error}")
            return json.dumps({"error": str(error)})
    
    def fetch_file_statuses_by_user_id(self, user_id):
        """
        Fetch the statuses of files by user ID.

        Args:
            user_id (str): ID of the user.

        Returns:
            str: JSON string containing the fetched file statuses or error message.
        """
        try:
            with self.connection.connect() as conn:
                fetch_query = sqlalchemy.text(self.gsql_config['files_status_by_user_id'])
                records = conn.execute(fetch_query, {'user_id': user_id}).fetchall()
                logger.info(f"Fetched {len(records)} file statuses, user_id: {user_id}")
                return json.dumps([{
                    "file_name": record[0],
                    "status": record[1],
                } for record in records])
        except Exception as error:
            logger.error(f"Error: Could not fetch file statuses\n{error}")
            return json.dumps({"error": str(error)})
        
    def delete_file(self, file_ids, user_id):
        """
        Delete a file from the database.

        Args:
            file_ids (list): List of file IDs to delete.

        Returns:
            str: JSON string containing the result of the deletion or error message.
        """
        try:
            with self.connection.connect() as conn:
                delete_query = sqlalchemy.text(self.gsql_config['delete_files'])
                conn.execute(delete_query, {'file_ids': file_ids, 'user_id': user_id})
                conn.commit()
                logger.info(f"Deleted {len(file_ids)} documents")
                logger.info(f"Documents deleted successfully, file_ids: {file_ids}, user_id: {user_id}")
                return json.dumps({
                    "message": "Documents deleted successfully", 
                    "file_ids": file_ids,
                    "user_id": user_id
                })
        except Exception as error:
            logger.error(f"Error: Could not delete documents\n{error}")
            return json.dumps({"error": str(error)})
        
    def connect(self) -> sqlalchemy.engine.base.Engine:
        """
        Initializes a connection pool for a Cloud SQL instance of Postgres.
        Uses the Cloud SQL Python Connector package.
        """

        creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        info = json.loads(creds)
        credentials = service_account.Credentials.from_service_account_info(info)
        
        connector = Connector(
            ip_type="public",  # can also be "private" or "psc"
            enable_iam_auth=False,
            timeout=30,
            credentials=credentials, # google.auth.credentials.Credentials
            refresh_strategy="lazy",  # can be "lazy" or "background"            
        )

        def getconn() -> pg8000.dbapi.Connection:
            conn: pg8000.dbapi.Connection = connector.connect(
                self.host,
                "pg8000",
                user=self.user,
                password=self.password,
                db=self.database,
                ip_type=IPTypes.PUBLIC,
            )
            return conn

        pool = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=getconn,
        )
        self.connection = pool