from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    status,
    Request,
    UploadFile,
    File,
    Form,
)


from datetime import datetime, timedelta
from typing import Union, Optional
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse

from pydantic import BaseModel
import json

from open_webui.apps.webui.models.files import (
    Files,
    FileForm,
    FileModel,
    FileModelResponse,
)
from open_webui.utils.utils import get_verified_user, get_admin_user
from open_webui.constants import ERROR_MESSAGES

from importlib import util
import os
import uuid
import os, shutil, logging, re


from open_webui.config import UPLOAD_DIR
from open_webui.env import SRC_LOG_LEVELS

log = logging.getLogger(__name__)
log.setLevel(SRC_LOG_LEVELS["MODELS"])


router = APIRouter()

############################
# Upload File
############################

from open_webui.apps.webui.routers.loader.classes.PDFLoader import PDFLoader
from open_webui.apps.webui.routers.loader.classes.DOCXLoader import DOCXLoader
from open_webui.apps.webui.routers.loader.classes.XLSXLoader import XLSXLoader
from open_webui.apps.webui.routers.loader.classes.PPTXLoader import PPTXLoader
from open_webui.apps.webui.routers.loader.classes.MSGLoader import MSGLoader

from typing import Dict, List, Union
from open_webui.apps.webui.routers.loader.classes.Chunk import Chunk

import os
import asyncio
import traceback


def format_chunks(chunks: List[Chunk]) -> str:
    if not chunks:
        return ""
    
    # 假設所有 chunks 的 metadata['page_title'] 和 metadata['source'] 都相同
    #title = chunks[0].metadata['page_title']
    source = chunks[0].metadata['source']
    
    # 組合內文
    content = "\n".join(chunk.content for chunk in chunks)
    
    # 組合完整輸出
    formatted_output = f"檔名 {source}\n\n內文:\n{content}"
    
    return formatted_output

@router.post("/")
def upload_file(file: UploadFile = File(...), user=Depends(get_verified_user)):
    log.info(f"file.content_type: {file.content_type}")

    # Allowed file extensions and size limit
    allowed_extensions = {".msg", ".pdf", ".pptx", ".docx", ".txt", ".md"}
    max_file_size = 10 * 1024 * 1024  # 10 MB

    try:
        # Validate file extension
        unsanitized_filename = file.filename
        filename = os.path.basename(unsanitized_filename)
        ext = os.path.splitext(filename)[1].lower()

        if ext not in allowed_extensions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Unsupported file format. Allowed formats: .msg, .pdf, .pptx, .docx, .txt, .md"
            )

        # Replace filename with UUID
        id = str(uuid.uuid4())
        name = filename
        filename = f"{id}_{filename}"
        file_path = f"{UPLOAD_DIR}/{filename}"

        # Handling .txt and .md files
        if ext in ['.txt', '.md']:
            # Process as text-based content (original handling)    
            # Validate file size
            contents = file.file.read()

            
            # Save file to disk
            with open(file_path, "wb") as f:
                f.write(contents)
                f.close()

            content = contents.decode("utf-8")
            # Insert into database (as per original logic)
            file = Files.insert_new_file(
                user.id,
                FileForm(
                    **{
                        "id": id,
                        "filename": filename,
                        "meta": {
                            "name": name,
                            "content_type": file.content_type,
                            "size": len(contents),
                            "path": file_path,
                        },
                    }
                ),
            )
        else:
            
            loaders = {
                'docx': DOCXLoader,
                'pptx': PPTXLoader,
                #'xlsx': XLSXLoader,
                'pdf': PDFLoader,
                'msg': MSGLoader
            }

            # Handling other file types with loaders
            LoaderClass = loaders.get(ext.lstrip('.'))  # Get loader class based on extension
         
            if LoaderClass:

                raw_content = file.file.read()
                
                # Save file to disk
                with open(file_path, "wb") as f:
                    f.write(raw_content)
                    f.close()

                loader = LoaderClass(directory=f"{UPLOAD_DIR}")
                loader._load_file(file_path=file_path)
                chunks = loader.loaded_files[file_path]

                all_chunks = []

                for chunk in chunks:
                    all_chunks.append(chunk)

                # Format the loaded content
                content = format_chunks(all_chunks)

                 # Validate file extension
                filename = os.path.basename(name)
                ext = os.path.splitext(filename)[1].lower()
                base_filename = os.path.splitext(filename)[0]  # Original filename without extension


                # Save extracted content as a .txt file
                extracted_filename = f"{id}_{base_filename}.txt"
                extracted_file_path = f"{UPLOAD_DIR}/{extracted_filename}"
                with open(extracted_file_path, "w", encoding="utf-8") as f:
                    f.write(content)

                # Update path in the database for the new .txt file
                file = Files.insert_new_file(
                    user.id,
                    FileForm(
                        **{
                            "id": id,
                            "filename": extracted_filename,
                            "meta": {
                                "name": f"{base_filename}.txt",
                                "content_type": "text/plain",
                                "size": len(content),
                                "path": extracted_file_path,
                            },
                        }
                    ),
                )
            else:
                file = None

        if file:
            return file
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ERROR_MESSAGES.DEFAULT("Error uploading file"),
            )

    except Exception as e   :
        log.exception(e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ERROR_MESSAGES.DEFAULT(str(e)),
        )



############################
# List Files
############################


@router.get("/", response_model=list[FileModel])
async def list_files(user=Depends(get_verified_user)):
    files = Files.get_files()
    return files


############################
# Delete All Files
############################


@router.delete("/all")
async def delete_all_files(user=Depends(get_admin_user)):
    result = Files.delete_all_files()

    if result:
        folder = f"{UPLOAD_DIR}"
        try:
            # Check if the directory exists
            if os.path.exists(folder):
                # Iterate over all the files and directories in the specified directory
                for filename in os.listdir(folder):
                    file_path = os.path.join(folder, filename)
                    try:
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                            os.unlink(file_path)  # Remove the file or link
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)  # Remove the directory
                    except Exception as e:
                        print(f"Failed to delete {file_path}. Reason: {e}")
            else:
                print(f"The directory {folder} does not exist")
        except Exception as e:
            print(f"Failed to process the directory {folder}. Reason: {e}")

        return {"message": "All files deleted successfully"}
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ERROR_MESSAGES.DEFAULT("Error deleting files"),
        )


############################
# Get File By Id
############################


@router.get("/{id}", response_model=Optional[FileModel])
async def get_file_by_id(id: str, user=Depends(get_verified_user)):
    file = Files.get_file_by_id(id)

    if file:
        return file
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ERROR_MESSAGES.NOT_FOUND,
        )


############################
# Get File Content By Id
############################


@router.get("/{id}/content", response_model=Optional[FileModel])
async def get_file_content_by_id(id: str, user=Depends(get_verified_user)):
    file = Files.get_file_by_id(id)

    if file:
        file_path = Path(file.meta["path"])

        # Check if the file already exists in the cache
        if file_path.is_file():
            print(f"file_path: {file_path}")
            return FileResponse(file_path)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ERROR_MESSAGES.NOT_FOUND,
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ERROR_MESSAGES.NOT_FOUND,
        )


@router.get("/{id}/content/{file_name}", response_model=Optional[FileModel])
async def get_file_content_by_id(id: str, user=Depends(get_verified_user)):
    file = Files.get_file_by_id(id)

    if file:
        file_path = Path(file.meta["path"])

        # Check if the file already exists in the cache
        if file_path.is_file():
            print(f"file_path: {file_path}")
            return FileResponse(file_path)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ERROR_MESSAGES.NOT_FOUND,
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ERROR_MESSAGES.NOT_FOUND,
        )


############################
# Delete File By Id
############################


@router.delete("/{id}")
async def delete_file_by_id(id: str, user=Depends(get_verified_user)):
    file = Files.get_file_by_id(id)

    if file:
        result = Files.delete_file_by_id(id)
        if result:
            return {"message": "File deleted successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ERROR_MESSAGES.DEFAULT("Error deleting file"),
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ERROR_MESSAGES.NOT_FOUND,
        )
