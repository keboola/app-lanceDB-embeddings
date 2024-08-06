import os
import zipfile
import pandas as pd
import lancedb
from lancedb.pydantic import LanceModel, Vector
from openai import OpenAI
import csv
import dataclasses
from typing import List
import json
import os
import requests.exceptions

from keboola.component.base import ComponentBase, sync_action
from keboola.component.sync_actions import ValidationResult, MessageType
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from kbcstorage.tables import Tables
from kbcstorage.client import Client

class EmbeddingComponent(ComponentBase):
    def __init__(self):
        super().__init__()
        self.client = None
        self.db = None
        self.table = None
        self.model = None
        self.vector_size = None

    def configure(self):
        params = self.configuration.parameters
        self.model = params.get('model', 'text-embedding-3-large')
        api_key = params.get('#openai_api_key')
        
        if not api_key:
            raise UserException("OpenAI API key is missing from the configuration.")

        os.environ["OPENAI_API_KEY"] = api_key
        self.client = OpenAI()

        os.makedirs("data/out/files", exist_ok=True)
        self.db = lancedb.connect("data/out/files")

    def get_embedding(self, text):
        text = text.replace("\n", " ")
        return self.client.embeddings.create(input=[text], model=self.model).data[0].embedding

    def create_table(self):
        class Words(LanceModel):
            text: str
            vector: Vector(self.vector_size)

        self.table = self.db.create_table("embedded", schema=Words, mode="overwrite")

    def process_data(self):
        input_table = self.get_input_table_definition()
        df = pd.read_csv(input_table.full_path)

        if 'bodyData' not in df.columns:
            raise UserException("'bodyData' column not found in the input CSV file")

        data = []
        for count, entry in enumerate(df['bodyData'], 1):
            embedding = self.get_embedding(entry)
            data.append({"text": entry, "vector": embedding})
            print(f"Added: {count}")

        print("Adding to table")
        try:
            self.table.add(data)
            print("Data added successfully")
        except Exception as e:
            raise UserException(f"Error adding data to table: {e}")

    def export_data(self):
        print("Exporting data to CSV")
        try:
            all_data = self.table.to_pandas()
            vector_df = pd.DataFrame(all_data['vector'].tolist(), columns=[f'vector_{i}' for i in range(self.vector_size)])
            export_df = pd.concat([all_data['text'], vector_df], axis=1)
            
            output_table = self.create_out_table_definition('embedded_data_with_vectors.csv')
            export_df.to_csv(output_table.full_path, index=False)
            print("Data exported successfully")
        except Exception as e:
            raise UserException(f"Error exporting data to CSV: {e}")

    def zip_lance_directory(self):
        print("Zipping the embedded.lance directory")
        try:
            lance_dir = "data/out/files/embedded.lance"
            zip_path = "data/out/files/embedded_lance.zip"
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(lance_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, lance_dir)
                        zipf.write(file_path, arcname)
            
            print(f"Successfully zipped embedded.lance to {zip_path}")
        except Exception as e:
            raise UserException(f"Error zipping embedded.lance: {e}")

    def run(self):
        self.configure()
        self.create_table()
        self.process_data()
        self.export_data()
        self.zip_lance_directory()

    @staticmethod
    def get_vector_size(model_name):
        model_sizes = {
            'text-embedding-3-small': 1536,
            'text-embedding-3-large': 3072,
            'text-embedding-ada-002': 1536
        }
        return model_sizes.get(model_name, 1536)

    def validate_configuration(self):
        required_params = ['model', '#openai_api_key']
        for param in required_params:
            if param not in self.configuration.parameters:
                raise UserException(f"'{param}' is missing from the configuration parameters")

    def get_input_table_definition(self):
        tables = self.get_input_tables_definitions()
        if len(tables) != 1:
            raise UserException("Exactly one input table is required.")
        return tables[0]

    @sync_action('getVectorSize')
    def get_vector_size_action(self):
        params = self.configuration.parameters
        model = params.get('model', 'text-embedding-3-large')
        vector_size = self.get_vector_size(model)
        self.vector_size = vector_size
        return {'vector_size': vector_size}

if __name__ == "__main__":
    try:
        comp = EmbeddingComponent()
        comp.execute_action()
    except UserException as e:
        print(f"User Exception: {e}")
        exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        exit(2)