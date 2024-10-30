import os
import json
import asyncio
import nest_asyncio
import traceback
from typing import Optional
from rich import print as rprint
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Union
from pydantic import BaseModel, field_validator, model_validator, PositiveInt, HttpUrl
from open_webui.apps.webui.routers.loader.classes.Chunk import Chunk

class BaseLoader(ABC):
    def __init__(self,
                 directory: str,
                 recursive: bool = True,
                 extensions: list[str] = None,
                 encoding: str = 'utf-8',
                 exclude_files: list[str] = None,
                 num_threads: int = 1,
                 aiil: bool = False,
                 restructure_system: str | None = None,
                 hitl: bool = False,
                 verbose: bool = False) -> None:
        
        if not os.path.isdir(directory):
            raise TypeError(f"`directory` must be a path of a directory, got {self.directory}")
        
        self.directory = directory
        self.recursive = recursive
        self.extensions = extensions
        self.encoding = encoding
        self.exclude_files = exclude_files
        self.num_threads = num_threads
        self.aiil = aiil
        self.restructure_system = restructure_system
        self.hitl = hitl
        self.verbose = verbose

        if self.exclude_files == None:
            self.exclude_files = []

        self.loaded_files: Dict[str, List[Chunk]] = {}

    # 定義一個內部函式來讀取文件內容
    @abstractmethod
    async def _load_file(self, file_path, temp_file_dir: Optional[str] = None):
        pass


    # 定義一個內部函式來判斷是否應該加載文件
    def _should_load_file(self, file:str):
        if self.exclude_files and os.path.basename(file) in self.exclude_files:
            return False
        if self.extensions and not any(file.endswith(ext) for ext in self.extensions):
            return False
        return True


    def load_nonsyn(self):
        """根據給定的參數從指定目錄載入檔案.

        Returns:
            Dict[str, List[Chunk]]: 一個字典, 其中key是檔案路徑, value是檔案的各個分塊.
        """

        # 如果包含子目錄，則遞歸遍歷目錄中的所有文件
        if self.recursive:
            for root, _, files in os.walk(self.directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    if self._should_load_file(file_path):
                        #print(file_path)
                        self._load_file(file_path=file_path)
        # 如果不包含子目錄，則直接遍歷當前目錄中的所有文件
        else:
            for file in os.listdir(self.directory):
                file_path = os.path.join(self.directory, file)
                if os.path.isfile(file_path) and self._should_load_file(file_path):
                        self._load_file(file_path=file_path)


        return self.loaded_files


    async def load(self):
        """根據給定的參數從指定目錄載入檔案.

        Returns:
            Dict[str, List[Chunk]]: 一個字典, 其中key是檔案路徑, value是檔案的各個分塊.
        """

        # 如果包含子目錄，則遞歸遍歷目錄中的所有文件
        if self.recursive:
            print('base loader load.')
            for root, _, files in os.walk(self.directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    if self._should_load_file(file_path):
                        print(file_path)
                        await self._load_file(file_path=file_path)
        # 如果不包含子目錄，則直接遍歷當前目錄中的所有文件
        else:
            for file in os.listdir(self.directory):
                file_path = os.path.join(self.directory, file)
                if os.path.isfile(file_path) and self._should_load_file(file_path):
                        await self._load_file(file_path=file_path)

        # 如果需要多線程，則使用 ThreadPoolExecutor 來並行加載文件
        if self.num_threads > 1:
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                if self.recursive:
                    for root, _, files in os.walk(self.directory):
                        for file in files:
                            file_path = os.path.join(root, file)
                            if self._should_load_file(file_path):
                                await self._load_file(file_path=file_path)
                else:
                    for file in os.listdir(self.directory):
                        file_path = os.path.join(self.directory, file)
                        if os.path.isfile(file_path) and self._should_load_file(file_path):
                            await self._load_file(file_path=file_path)
        print('here_end')
        return self.loaded_files


    async def _human_in_the_loop(self, content: str, source: str) -> str:
        orig_content=content
        
        edited_content = HumanCheckPrompt(message=f"\n<<< Check/Edit content of chunk({source}) >>>\n\n",
                                          validator=InputValidator(),
                                          default=orig_content)
        
        print_centered_text(' Content ', padding_char='=')
        rprint(edited_content)
        print_centered_text('', padding_char='=')
        
        while True:
            confirm = input("\nSave the change of the content for this chunk?\n[yes/no]>>> ").strip().lower()
            if confirm in ['y', 'yes']:
                return edited_content
            elif confirm in ['n', 'no']:
                return orig_content
            else:
                print(Fore.YELLOW + "Invalid input" + Fore.RESET)


    def _build_chunk(self, id: str, content: str, metadata: Dict) -> Chunk:
        chunk_config = {
            'id': id,
            'content': content,
            'metadata': metadata
            }
        return Chunk(**chunk_config)

        
        
