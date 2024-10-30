from pydantic import BaseModel, Field, NonNegativeInt
from typing import Dict
import os
import json
from tqdm import tqdm
from rich import print
from typing import Dict, List
import pickle
import uuid

class Chunk(BaseModel):
    id: str = Field(..., description='UUID of the chunk')
    content: str = Field('', description='Content of the chunk')
    metadata: Dict[str, str|NonNegativeInt] = Field({}, description='Metadata of the chunk')

import yaml
import re

def save_chunk(chunk: Chunk, chunk_file: str):
    with open(chunk_file, 'wb') as file:
        pickle.dump(chunk, file)

def load_chunk(chunk_file: str) -> Chunk:
    with open(chunk_file, 'rb') as file:
        loaded_chunk = pickle.load(file)
        return loaded_chunk

def save_chunks(chunks: List[Chunk], chunks_file: str):
    with open(chunks_file, 'wb') as file:
        pickle.dump(chunks, file)

def load_chunks(chunks_file: str) -> List[Chunk]:
    with open(chunks_file, 'rb') as file:
        loaded_chunks = pickle.load(file)
        return loaded_chunks

def save_chunks_json(chunks: List[Chunk], chunks_file: str):
    # Convert the list of Chunk objects to a list of dictionaries
        chunks_dict = [chunk.model_dump() for chunk in chunks]

        # Save the list of dictionaries to a JSON file
        with open(chunks_file, 'w') as f:
            json.dump(chunks_dict, f, indent=4)

def load_chunks_json(chunks_file: str) -> List[Chunk]:
    # Load the JSON file into a list of dictionaries
    with open(chunks_file, 'r') as f:
        chunks_dict = json.load(f)

    # Convert the list of dictionaries back to a list of Chunk objects
    chunks: List[Chunk] = [Chunk(**chunk) for chunk in chunks_dict]
    return chunks

def chunk_to_markdown(chunk: Chunk, filename: str):
    """
    将 Chunk 实例输出为带有 YAML 前置数据的 Markdown 文件。
    """
    # 准备 YAML 前置数据
    front_matter = {
        'id': chunk.id,
        'metadata': chunk.metadata
    }
    yaml_str = yaml.dump(front_matter, sort_keys=False, allow_unicode=True)

    # 准备 Markdown 内容
    markdown_content = f"""---
{yaml_str}---
{chunk.content}
    """
    # 写入文件
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(markdown_content)

def markdown_to_chunk(filename: str) -> Chunk:
    """
    从带有 YAML 前置数据的 Markdown 文件中读取内容并创建 Chunk 实例。
    检查文件格式的正确性。
    """
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()

    # 使用正则表达式提取 YAML 前置数据和内容
    match = re.match(r'^---\n(.*?)\n---\n(.*)', content, re.DOTALL)
    if not match:
        raise ValueError("Markdown 文件格式不正确，未找到 YAML 前置数据。")

    yaml_content = match.group(1)
    chunk_content = match.group(2)

    # 解析 YAML 前置数据
    try:
        front_matter = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        raise ValueError(f"YAML 前置数据解析错误: {e}")

    # 检查必要的键是否存在
    if 'id' not in front_matter or 'metadata' not in front_matter:
        raise ValueError("YAML 前置数据缺少 'id' 或 'metadata' 键。")

    chunk_id = front_matter['id']
    metadata = front_matter['metadata']

    # 创建 Chunk 实例
    chunk = Chunk(id=chunk_id, content=chunk_content.strip(), metadata=metadata)
    return chunk


def generate_unique_uuid(generated_uuids=None) -> str:
    new_uuid = str(uuid.uuid4())

    if generated_uuids is not None:
        while new_uuid in generated_uuids:
            new_uuid = str(uuid.uuid4())

    return new_uuid