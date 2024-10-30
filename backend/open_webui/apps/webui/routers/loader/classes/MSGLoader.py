from open_webui.apps.webui.routers.loader.classes.BaseLoader import BaseLoader
import os
import glob
import uuid
from outlook_msg import Message
import re
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
from spacy.lang.en import English
from unstructured.cleaners.core import clean_bullets


from spacy.lang.en import English
import shutil  
from typing import Dict, List, Union
import traceback
import re
from open_webui.apps.webui.routers.loader.classes.Chunk import Chunk
from open_webui.apps.webui.routers.loader.classes.Chunk import generate_unique_uuid

class MSGLoader(BaseLoader):
    def __init__(self,
                 directory: str,
                 recursive: bool = True,
                 exclude_files: list[str] = None,
                 num_threads: int = 1,
                 aiil: bool = False,
                 hitl: bool = False,
                 verbose: bool = False) -> None:

        super().__init__(directory=directory,
                         recursive=recursive,
                         extensions=['.msg'],
                         exclude_files=exclude_files,
                         num_threads=num_threads,
                         aiil=aiil,
                         hitl=hitl,
                         verbose=verbose)

        self.nlp = English()
        self.nlp.add_pipe("sentencizer")

        if not os.path.isdir(directory):
            raise Exception(f"`directory` must be a path of a directory, got {self.directory}")

    def _load_file(self, file_path: str):
        try:

            chunks = self._msg_to_chunks(file_path)
            if chunks:
                self.loaded_files[file_path] = chunks
        except Exception as e:
            print(f"\n***** Error reading {file_path}: {e} *****\n")

    def _msg_to_chunks(self, file_path: str):
        try:
            msg_content, metadata = self._process_msg(file_path)

            
            document_id = ''
            #print(file_path)
            match = re.search(r'attahments/(\d+)/', file_path)
            if match:
                document_id = match.group(1)

            metadata['confluence_id'] = document_id

            # Create Chunk
            chunk = Chunk(id=generate_unique_uuid())
            chunk.content = msg_content
            chunk.metadata = metadata

            return [chunk]

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"\n***** Error processing {file_path}: {e} *****\n")
            return None

    def safe_decode(self, value, encoding='utf-8'):
        try:
            return value.decode(encoding, errors='ignore')
        except Exception as e:
            print(f"Decoding error: {e}")
            return ""

    def _process_msg(self, file_path):
        sender = ''
        date = ''
        with open(file_path, 'rb') as f:
            msg = Message(f)
            plain_body = self.safe_decode(msg.body.encode('utf-8')) if msg.body else ''
            subject = self.safe_decode(msg.subject.encode('utf-8'))
            #sender = msg.sender_email  # Corrected attribute name
            #date = msg.date            # Corrected attribute name
            #html_body = msg.html_body or ''

        cleaned_texts = ""
        tolerance = 10
        df_i = 0
        repeat = ""
        matches = []

        # Subject
        subject_text = "# " + subject + "\n"
        if subject_text:
            cleaned_texts += subject_text

        msg_content = plain_body
        metadata = {
            "ID": str(uuid.uuid1()),
            "subject": subject,
            "sender": sender,
            "date": str(date),
            "source": file_path
        }

        return msg_content, metadata

    def _process_msg_to_table(self, match, df_i):
        html_content = str(match)
        html_table = StringIO(html_content)
        dfs = pd.read_html(html_table)
        if df_i < len(dfs):
            df = dfs[df_i]
            json_table = df.to_json(orient='records', force_ascii=False)
            return json_table
        else:
            raise ValueError("DataFrame index out of range")

    def _convert_to_markdown(self, msg_content):

        lines = msg_content.strip().split('\n')
        processed_lines = ""
        for line in lines:
            if line.startswith("["):
                processed_line = "```json\n" + line + "\n```"
                processed_lines += processed_line + '\n'
                continue
            elif line.startswith("#"):
                processed_line = line

            else:
                processed_line = line

            processed_lines += processed_line + '\n'

        msg_content = processed_lines

        return msg_content

    def _filter_metadata(self, metadata_dict):
        return metadata_dict
