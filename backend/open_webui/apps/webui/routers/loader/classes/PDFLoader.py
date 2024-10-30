from open_webui.apps.webui.routers.loader.classes.BaseLoader import BaseLoader
import os
# 讓 UNSTRUCTED 不要去連線
# 設置 SCARF_NO_ANALYTICS 為 True
os.environ["SCARF_NO_ANALYTICS"] = "true"

# 設置 DO_NOT_TRACK 為 True
os.environ["DO_NOT_TRACK"] = "true"

# 設定 NKLT DATA 所在地
os.environ["NLTK_DATA"] = "/usr/local/nltk_data"

from unstructured.partition.pdf import partition_pdf
import unstructured
from unstructured.cleaners.core import clean_bullets

from spacy.lang.en import English
import shutil  
from typing import Dict, List, Union
import traceback
import re
from open_webui.apps.webui.routers.loader.classes.Chunk import Chunk
from open_webui.apps.webui.routers.loader.classes.Chunk import generate_unique_uuid


class PDFLoader(BaseLoader):
    def __init__(self,
                 directory: str,
                 only_latest_content: bool = True,
                 recursive: bool = True,
                 encoding: str = 'utf-8',
                 exclude_files: list[str] = None,
                 num_threads: int = 1,
                 aiil: bool = False,
                 hitl: bool = False,
                 verbose: bool = False) -> None:
        
        # 在子類的構造函數內部調用super().__init__來初始化基類的屬性，並在之後初始化子類特有的屬性。
        super().__init__(directory=directory,
                         recursive=recursive, 
                         extensions=['.pdf'], 
                         encoding=encoding, 
                         exclude_files=exclude_files, 
                         num_threads=num_threads,
                         aiil=aiil,
                         hitl=hitl,
                         verbose=verbose)

        self.nlp = English()
        self.nlp.add_pipe("sentencizer")
        
        if not os.path.isdir(directory):
            raise Exception(f"`directory` must be a path of a directory, got {self.directory}")


    # 定義一個內部函式來讀取文件內容
    def _load_file(self, file_path:str):
        try:
            print(f'loading pdf file: {file_path}')
            chunks = self._pdf_to_chunks(file_path)
            

            if chunks:
                self.loaded_files[file_path] = chunks


        except Exception as e:
            print(f"\n***** Error reading {file_path}: {e} *****\n")
    

    def _from_structed_pages_to_chunks(self, structed_pages, file_path):

        document_id = ''
        match = re.search(r'attahments/(\d+)/', file_path)
        if match:
            document_id = match.group(1)


        chunks = []

        for page in structed_pages:

            chunk = Chunk(id=generate_unique_uuid())

            chunk.content = '\n'.join(page['content'])
            
            page_title = page['meta']['page_titles']
            source = page['meta']['pdf_file_name']

            metadata = {
                'hierarchy_info':'',
                'page_title': page_title,
                'author' : '',
                'editor' : '',
                'created_date' : '',
                'create_date_timestamp' : '',
                'attachments' : '',
                'source' : source,
                'id' : '',
                'confluence_link' : '',
                'confluence_id' : document_id
            }

            chunk.metadata = metadata

            chunks.append(chunk)

        return chunks

    def _from_unstructed_elements_to_structed_pages(self, pdf_elements, file_path):
        # get maximun page number
        max_number = 0

        for element in pdf_elements:
            page_number = element.metadata.page_number
            if page_number > max_number:
                max_number = page_number

        if max_number > 0:

            # Now, copy images from unstructured_image_dir to your desired directory
            # For this example, we'll copy them to the same directory as the PDF file
            desired_image_dir = os.path.dirname(file_path)
            for root, dirs, files in os.walk(self.unstructured_image_dir):
                for file in files:
                    # Copy each image file
                    src_path = os.path.join(root, file)
                    dst_path = os.path.join(desired_image_dir, file)
                    shutil.copy2(src_path, dst_path)
                    # Append image reference to content

            pdf_file_name = pdf_elements[0].metadata.filename

            structed_pages = []

            for i in range(max_number):
                item_text = ''
                cur_page_number = i + 1

                page_titles = []
                page_contents = []
                
                for element in pdf_elements:
                    if element.metadata.page_number == cur_page_number:
                        if element.category == 'UncategorizedText':
                            continue
                        elif isinstance(element, unstructured.documents.elements.Image):
                            pass
                        
                        elif isinstance(element, unstructured.documents.elements.NarrativeText):
                            doc = self.nlp(element.text)
                            sentences = [sent.text.strip() for sent in doc.sents]
                            result_text = '\n\n'.join(sentences)
                            page_contents.append(result_text)
                        elif isinstance(element, unstructured.documents.elements.Title):
                            page_titles.append(element.text)
                            page_contents.append(element.text)
                        elif isinstance(element, unstructured.documents.elements.Table):

                            #table_dict = pd.read_html(element.metadata.text_as_html)[0].to_dict(orient='records')
                            #table_json = json.dumps(table_dict, ensure_ascii=False, indent=4)
                            
                            page_contents.append('<html table>')

                            page_contents.append(element.metadata.text_as_html)

                        elif isinstance(element, unstructured.documents.elements.ListItem):
                            page_contents.append(clean_bullets(element.text))
                        else:
                            page_contents.append(element.text)

                whole_titles = ''

                if any(page_titles):
                    for index, value in enumerate(page_titles):
                        if index == len(page_titles) - 1:
                            whole_titles = whole_titles + f'{value}'
                        else:
                            whole_titles = whole_titles + f'{value} , '

                page_meta = {'pdf_file_name':pdf_file_name,'page_number':f'{cur_page_number}', 'page_titles':f'{whole_titles}'} 

                structed_pages.append({'page_number':cur_page_number, 'titles':page_titles, 'content': page_contents, 'meta':page_meta})

            return structed_pages

        return None


    def _pdf_to_chunks(self, file_path:str):
        try:
            self.unstructured_image_dir = os.path.join(os.path.dirname(file_path), 'unstructured_images')
            os.makedirs(self.unstructured_image_dir, exist_ok=True)

            elements = partition_pdf(
                filename=file_path,                  # mandatory
                strategy="hi_res",                                     # mandatory to use ``hi_res`` strategy
                extract_images_in_pdf=True,                            # mandatory to set as ``True``
                extract_image_block_types=["Image", "Table"],          # optional
                extract_image_block_to_payload=False,                  # optional
                #extract_image_block_output_dir="./pdf_test_images",  # optional - only works when ``extract_image_block_to_payload=False``
                infer_table_structure=True,
                extract_image_block_output_dir=self.unstructured_image_dir  # Set the image output directory
                )

            
            pages = self._from_unstructed_elements_to_structed_pages(pdf_elements=elements, file_path=file_path)

            chunks = self._from_structed_pages_to_chunks(structed_pages= pages, file_path=file_path)
            
            return chunks

        except Exception as e:
            traceback.print_exc()
            print(f"\n***** Error reading {file_path}: {e} *****\n")
            return None

    def _filter_metadata(self, metadata_dict: Dict[str, Union[str, List[str]]]) -> Dict[str, str]:
        """將metadata中的鍵值對中, 數值型態為`list[str]`的數值轉成`str`並以逗號做分隔

        Args:
            metadata_dict (Dict[str, Union[str, List[str]]]): 元數據

        Returns:
            Dict[str, str]: 過濾後的元數據
        """
        for key, value in metadata_dict.items():
            if isinstance(value, list):
                metadata_dict[key] = ','.join(value)
        return metadata_dict
