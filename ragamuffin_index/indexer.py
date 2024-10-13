import os
import sys
import uuid
import logging
from neo4j import GraphDatabase
from llama_parse import LlamaParse
from llama_index.core import Settings
from llama_index.llms.openai import OpenAI

abs_path = os.path.abspath(__file__)
dir_name = os.path.dirname(abs_path)
sys.path.append(os.path.dirname(dir_name))
from ragamuffin_core.common import rds_helper

from ragamuffin_core.aws_s3_helper import AwsS3Helper
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core.node_parser import MarkdownElementNodeParser

logger = logging.getLogger(__name__)

class Indexer:

    def __init__(self):
        """
        Initializes the class with various components necessary for parsing, language models, embeddings, 
        and database connectivity.
        """
        
        self.llama_parse = LlamaParse(
            result_type="markdown",
            gpt4o_mode=True,
            gpt4o_api_key=os.environ.get("OPENAI_API_KEY"),
            num_workers=8,
        )
        self.llm = OpenAI(model="gpt-4")
        embed_model_id = os.environ.get("EMBED_MODEL_ID")
        self.embed_model = OpenAIEmbedding(model=embed_model_id)
        self.driver = GraphDatabase.driver(
            os.environ.get("NEO4J_URI"), 
            auth=(
                os.environ.get("NEO4J_USER"), 
                os.environ.get("NEO4J_PASSWORD")
            )
        )
        self.node_parser = MarkdownElementNodeParser(
            llm=self.llm, 
            num_workers=8
        )
        Settings.llm = self.llm
        Settings.embed_model = self.embed_model
    
    def init_schema(self):
        """
        Initializes the database schema by creating necessary constraints and indexes in the Neo4j database.

        This method executes a series of Cypher queries to enforce uniqueness constraints on specific node properties 
        and to create a vector index for efficient retrieval of embeddings.
        """
        logger.info("Initializing schema")
        try:
            init_schema = [
                "CREATE CONSTRAINT FOR (n:Node) REQUIRE n.node_id IS UNIQUE",
                "CREATE CONSTRAINT FOR (t:Text) REQUIRE t.text_id IS UNIQUE",  
                "CREATE VECTOR INDEX FOR (e:Embedding) ON (e.value) OPTIONS { indexConfig: {`vector.dimensions`: 1536, `vector.similarity_function`: 'cosine'}};"
            ]
            with self.driver.session() as session:
                for cypher in init_schema:
                    session.run(cypher)
            session.close()
            logger.info("Schema initialized")
        except Exception as e:
            logger.error(f"An error occurred while trying to initialize the schema: {e}")

    def input_nodes(self, session, nodes):
        """
        Indexes a list of nodes in the Neo4j database, including their textual content and embeddings.

        This method takes a list of nodes and stores them in the Neo4j graph database, linking each node
        with its associated text and embedding. Additionally, it creates relationships between the current 
        node and its previous or next nodes if such relationships exist.
        """
        logger.info("Index nodes in Neo4j")
        for node in nodes:
            node_id = node.node_id
            content=node.get_content()
            embedding = self.embed_model.get_text_embedding(content)
            session.run(
                """
                MERGE (n:Node {node_id: $node_id})
                CREATE (t:Text {text_id: $text_id, text: $text_value})
                CREATE (e:Embedding {value: $embed_value})
                CREATE (n)-[:HAS_TEXT]->(t)
                CREATE (n)-[:HAS_EMBEDDING]->(e)
                """, 
                node_id=node_id,
                text_id=str(uuid.uuid4()), 
                text_value=content, 
                embed_value=embedding
            )

            if node.next_node:
                session.run(
                    """
                    MERGE (p:Node {node_id: $node_id})
                    MERGE (n:Node {node_id: $next_id})
                    MERGE (n)<-[:NEXT]-(p);
                    """,
                    node_id=node.node_id,
                    next_id=node.next_node.node_id
                )

            if node.prev_node:
                session.run(
                    """
                    MERGE (p:Node {node_id: $node_id})
                    MERGE (n:Node {node_id: $prev_id})
                    MERGE (n)-[:NEXT]->(p);
                    """,
                    node_id=node.node_id,
                    prev_id=node.prev_node.node_id
                )
        logger.info("Nodes indexed in Neo4j")

    def index_message(self, message):
        """
        Indexes a document by parsing the content and creating a node in the Neo4j graph database.

        This method takes a message as an input, which contains the file path of the document 
        to be indexed. It then reads the content of the document, parses it using the Llama parser, 
        and creates a node in the Neo4j graph database representing the document.

        Parameters:
        message (dict): A message containing the file path of the document to be indexed.
        """
        try:
            file_path = message["file_path"]
            file_id = message["file_id"]
            user_id = message["user_id"]
            bucket = os.environ.get("AWS_BUCKET_NAME")
            logger.info(f"Bucket: {bucket}")
            local_folder = os.environ.get("LOCAL_FILES_PATH")
            logger.info(f"Local folder: {local_folder}")
            s3path = os.environ.get("AWS_FILES_PATH")
            logger.info(f"Indexing document: {file_path}")
            logger.info(f"Downloading file from S3: {s3path}")
            full_path = f"{s3path}/{os.path.basename(file_path)}"
            local_file = AwsS3Helper.download_file(bucket, full_path, local_folder)
            self.index(local_file)
            logger.info(f"Document indexed successfully: {file_path}")
            rds_helper.update_status_for_files([file_id], user_id, "indexed")
            self.delete_file(local_file)
            logger.info(f"File {local_file} deleted successfully.")
        except Exception as e:
            logger.error(f"An error occurred while trying to index the document: {e}")
    
    def index(self, path):
        logger.info(f"Uplaod file to Llama Parse file: {path}")
        document = self.llama_parse.load_data(path)
        nodes = self.node_parser.get_nodes_from_documents(document)
        base_nodes, objects = self.node_parser.get_nodes_and_objects(nodes)
        logger.info(f"File {path} parsed by Llama Parse")
        with self.driver.session() as session:
            logger.info("Base Nodes processing for Neo4j")
            self.input_nodes(session, base_nodes)
            logger.info("Objects processing for Neo4j")
            self.input_nodes(session, objects)
        session.close()

    def delete_file(self, path):
        """
        Deletes a file from the local filesystem.

        This method takes the file path as an input, determines the full path of the file
        by appending the base name of the file (from the given path) to the folder path 
        specified in the environment variable 'LOCAL_FILES_PATH'. It then checks if the 
        file exists at that location and removes it if found.

        Parameters:
        path (str): The file path (can be a relative or absolute path) to delete.
        """
        try:
            folder_path = os.environ.get("LOCAL_FILES_PATH")
            file_path = os.path.join(folder_path, os.path.basename(path))
            if os.path.exists(file_path):
                os.remove(f"{file_path}")
                logger.info(f"{file_path} has been removed successfully.")
        except Exception as e:
            logger.error(f"An error occurred while trying to delete {file_path}: {e}")