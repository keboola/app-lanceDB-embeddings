import csv
import logging
import os
import shutil
import zipfile
import lancedb

import pyarrow as pa
import pandas as pd

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from configuration import Configuration

from openai import OpenAI
class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self._configuration = None
        self.client = None

    def run(self):
        self.init_configuration()
        self.init_client()
        try:
            input_table = self._get_input_table()
            with open(input_table.full_path, 'r', encoding='utf-8') as input_file:
                reader = csv.DictReader(input_file)
                if self._configuration.outputFormat == 'lance':
                    lance_dir, table = self._initialize_lance_output(reader.fieldnames)
                    self._process_rows_lance(reader, table)
                elif self._configuration.outputFormat == 'csv':
                    self._process_rows_csv(reader)
        except Exception as e:
            raise UserException(f"Error occurred during embedding process: {str(e)}")

    def _initialize_lance_output(self, fieldnames):
        lance_dir = os.path.join(self.tables_out_path, 'lance_db')
        os.makedirs(lance_dir, exist_ok=True)
        db = lancedb.connect(lance_dir)
        schema = self._get_lance_schema(fieldnames)
        table = db.create_table("embeddings", schema=schema, mode="overwrite")
        return lance_dir, table

    def _process_rows_csv(self, reader):
        output_table = self._get_output_table()
        with open(output_table.full_path, 'w', encoding='utf-8', newline='') as output_file:
            fieldnames = reader.fieldnames + ['embedding']
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            writer.writeheader()
            self.row_count = 0
            for row in reader:
                self.row_count += 1
                text = row[self._configuration.embedColumn]
                embedding = self.get_embedding(text)
                row['embedding'] = embedding
                writer.writerow(row)
                
    def _process_rows_lance(self, reader, table, lance_dir):
        data = []
        self.row_count = 0
        for row in reader:
            self.row_count += 1
            text = row[self._configuration.embedColumn]
            embedding = self.get_embedding(text)
            lance_row = {**row, 'embedding': embedding}
            data.append(lance_row)
            if self.row_count % 1000 == 0:
                table.add(data)
                data = []
        if data:
            table.add(data)
        self._finalize_lance_output(lance_dir)

        
    def init_configuration(self):
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    def init_client(self):
        self.client = OpenAI(api_key=self._configuration.pswd_apiKey)

    def get_embedding(self, text):
        try:
            response = self.client.embeddings.create(input=[text], model=self._configuration.model)
            return response.data[0].embedding
        except Exception as e:
            raise UserException(f"Error getting embedding: {str(e)}")
        
    def _get_input_table(self):
        if not self.get_input_tables_definitions():
            raise UserException("No input table specified. Please provide one input table in the input mapping!")
        if len(self.get_input_tables_definitions()) > 1:
            raise UserException("Only one input table is supported")
        return self.get_input_tables_definitions()[0]
    
    def _get_output_table(self):        
        destination_config = self.configuration.parameters['destination']
        if not (out_table_name := destination_config.get("output_table_name")):
            out_table_name = f"app-embed-lancedb.csv"
        else:
            out_table_name = f"{out_table_name}.csv"

        return self.create_out_table_definition(out_table_name)
    
    def _get_lance_schema(self, fieldnames):
        schema = pa.schema([
            (name, pa.string()) for name in fieldnames
        ] + [('embedding', pa.list_(pa.float32()))])
        return schema
    
    def _finalize_lance_output(self, lance_dir):
        print("Zipping the Lance directory")
        try:
            zip_path = os.path.join(self.files_out_path, 'embeddings_lance.zip')
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(lance_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, lance_dir)
                        zipf.write(file_path, arcname)
            print(f"Successfully zipped Lance directory to {zip_path}")
            # Remove the original Lance directory
            shutil.rmtree(lance_dir)
        except Exception as e:
            print(f"Error zipping Lance directory: {e}")
            raise

if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)