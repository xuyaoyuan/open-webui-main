from open_webui.apps.webui.routers.loader.classes.BaseLoader import BaseLoader
import pandas as pd
import os
from openpyxl import load_workbook
from openpyxl.drawing.image import Image as XLImage

from spacy.lang.en import English
import shutil  
from typing import Dict, List, Union
import traceback
import re
from open_webui.apps.webui.routers.loader.classes.Chunk import Chunk
from open_webui.apps.webui.routers.loader.classes.Chunk import generate_unique_uuid

class XLSXLoader(BaseLoader):
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
                         extensions=['.xlsx'],
                         exclude_files=exclude_files,
                         num_threads=num_threads,
                         aiil=aiil,
                         hitl=hitl,
                         verbose=verbose)

        if not os.path.isdir(directory):
            raise Exception(f"`directory` must be a path of a directory, got {self.directory}")

    def _load_file(self, file_path: str):
        try:
            # Extract images from the Excel file
            # self._extract_images(file_path)
            # Convert Excel data to chunks
            chunks = self._xlsx_to_chunks(file_path)
            self.loaded_files[file_path] = chunks
        except Exception as e:
            print(f"\n***** Error reading {file_path}: {e} *****\n")

    def _extract_images(self, file_path: str):
        try:
            # Load the workbook with image preservation
            wb = load_workbook(file_path, keep_vba=False, data_only=True)
            image_dir = os.path.dirname(file_path)  # Same directory as the Excel file
            file_name_base = os.path.splitext(os.path.basename(file_path))[0]
            image_index = 1  # For unique image filenames

            # Iterate over each worksheet
            for sheet in wb.worksheets:
                # Access images in the worksheet
                for image in sheet._images:
                    # Construct image filename and path
                    image_filename = f"{file_name_base}_image_{image_index}.png"
                    image_path = os.path.join(image_dir, image_filename)
                    print(f"Saving image: {image_path}")

                    # Save the image
                    with open(image_path, 'wb') as img_file:
                        img_file.write(image._data())

                    image_index += 1
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"\n***** Error extracting images from {file_path}: {e} *****\n")

    def _xlsx_to_chunks(self, file_path: str):
        try:
            xls = pd.ExcelFile(file_path)
            chunks = []

            for sheet_name in xls.sheet_names:
                df = xls.parse(sheet_name)
                # 删除全空的行和列
                df.dropna(axis=0, how='all', inplace=True)
                df.dropna(axis=1, how='all', inplace=True)
                # 重置索引
                df.reset_index(drop=True, inplace=True)

                if df.empty:
                    continue  # 跳过空的工作表

                # 将 DataFrame 转换为 JSON 字符串
                content_str = df.to_json(orient='records', force_ascii=False)

                chunk = Chunk(id=generate_unique_uuid())
                chunk.content = content_str

                    
                document_id = ''
                #print(file_path)
                match = re.search(r'attahments/(\d+)/', file_path)
                if match:
                    #print('match')
                    document_id = match.group(1)

                metadata = {
                    'file_name': os.path.basename(file_path),
                    'sheet_name': sheet_name,
                    'source': os.path.basename(file_path),
                    'confluence_id' : document_id
                }
                chunk.metadata = self._filter_metadata(metadata)
                chunks.append(chunk)
            return chunks
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"\n***** Error processing {file_path}: {e} *****\n")
            return None

    def _filter_metadata(self, metadata_dict):
        return metadata_dict
