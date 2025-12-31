"""
Attachment processing utilities for handling PDFs and images.
Extracts content from PDFs and converts images to base64.
"""

import os
import logging
import base64
import re
from PIL import Image
import io
from ollama import Client

def sanitize_text(text):
    """
    Sanitize text by removing problematic characters and normalizing whitespace.
    
    Args:
        text: Input text string
        
    Returns:
        Sanitized text string
    """
    # Remove non-printable characters except common whitespace
    text = re.sub(r'[^\x20-\x7E\n\r\t]+', ' ', text)
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


def extract_pdf_content(pdf_path):
    """
    Extract text content from a PDF file using PyPDFLoader.
    
    Args:
        pdf_path: Path to PDF file
        
    Returns:
        Dictionary with 'content' and 'metadata' keys
    """
    try:
        from langchain_community.document_loaders import PyPDFLoader
        
        if not os.path.exists(pdf_path):
            logging.error(f"PDF file not found: {pdf_path}")
            return {"content": "", "metadata": []}
        
        # Load PDF using LangChain loader
        loader = PyPDFLoader(pdf_path)
        documents = loader.load()
        
        if not documents:
            logging.warning(f"No content extracted from PDF: {pdf_path}")
            return {"content": "", "metadata": []}
        
        # Extract text from all pages
        extracted_content = "\n".join(doc.page_content for doc in documents)
        
        # Extract metadata from all pages
        metadata = [doc.metadata for doc in documents]
        
        if not extracted_content.strip():
            logging.info(f"Extracted content is empty for PDF: {pdf_path}")
            return {"content": "", "metadata": metadata}
        
        logging.info(
            f"Extracted {len(extracted_content)} characters from PDF {pdf_path} "
            f"({len(documents)} pages)"
        )
        
        # Sanitize extracted text
        sanitized_content = sanitize_text(extracted_content)
        
        # Format as markdown code block for better parsing
        formatted_content = f"```pdf\n{sanitized_content}\n```"
        
        return {
            "content": formatted_content,
            "metadata": metadata
        }
        
    except ImportError as e:
        logging.error(f"PyPDFLoader not available: {str(e)}")
        return {"content": "Error: PDF processing library not available", "metadata": []}
    
    except Exception as e:
        logging.error(f"Error extracting PDF content from {pdf_path}: {str(e)}", exc_info=True)
        return {"content": f"Error extracting PDF: {str(e)}", "metadata": []}


def convert_image_to_base64(image_path):
    """
    Convert an image file to base64 encoded string.
    
    Args:
        image_path: Path to image file
        
    Returns:
        Base64 encoded string or empty string if error
    """
    try:
        if not os.path.exists(image_path):
            logging.error(f"Image file not found: {image_path}")
            return ""
        
        # Open image with PIL
        with Image.open(image_path) as img:
            # Convert to bytes
            img_byte_arr = io.BytesIO()
            
            # Determine format from image
            img_format = img.format if img.format else 'PNG'
            
            # Save to bytes
            img.save(img_byte_arr, format=img_format)
            img_data = img_byte_arr.getvalue()
            
            # Encode to base64
            base64_string = base64.b64encode(img_data).decode("utf-8")
            
            logging.info(
                f"Converted image {image_path} to base64 "
                f"(size: {len(img_data)} bytes, base64 length: {len(base64_string)})"
            )
            
            return base64_string
            
    except Exception as e:
        logging.error(f"Error converting image {image_path} to base64: {str(e)}", exc_info=True)
        return ""


def get_file_info(file_path):
    """
    Get basic information about a file.
    
    Args:
        file_path: Path to file
        
    Returns:
        Dictionary with file information
    """
    try:
        if not os.path.exists(file_path):
            return {
                "exists": False,
                "size": 0,
                "extension": "",
                "name": ""
            }
        
        file_stat = os.stat(file_path)
        file_name = os.path.basename(file_path)
        file_ext = os.path.splitext(file_name)[1]
        
        return {
            "exists": True,
            "size": file_stat.st_size,
            "extension": file_ext,
            "name": file_name,
            "full_path": os.path.abspath(file_path)
        }
        
    except Exception as e:
        logging.error(f"Error getting file info for {file_path}: {str(e)}")
        return {
            "exists": False,
            "size": 0,
            "extension": "",
            "name": "",
            "error": str(e)
        }


def validate_attachment(attachment_path, allowed_types=None):
    """
    Validate an attachment file.
    
    Args:
        attachment_path: Path to attachment file
        allowed_types: List of allowed MIME types or extensions
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if allowed_types is None:
        allowed_types = ['.pdf', '.png', '.jpg', '.jpeg', 'application/pdf', 'image/png', 'image/jpeg']
    
    try:
        if not os.path.exists(attachment_path):
            return False, "File does not exist"
        
        file_info = get_file_info(attachment_path)
        
        if file_info['size'] == 0:
            return False, "File is empty"
        
        # Check file size (max 10MB)
        max_size = 10 * 1024 * 1024  # 10MB
        if file_info['size'] > max_size:
            return False, f"File too large: {file_info['size']} bytes (max: {max_size} bytes)"
        
        # Check extension if allowed_types specified
        if allowed_types:
            extension = file_info['extension'].lower()
            if not any(ext in allowed_types or ext == extension for ext in allowed_types):
                return False, f"File type not allowed: {extension}"
        
        return True, "Valid"
        
    except Exception as e:
        logging.error(f"Error validating attachment {attachment_path}: {str(e)}")
        return False, f"Validation error: {str(e)}"


def extract_text_from_multiple_pdfs(pdf_paths):
    """
    Extract text content from multiple PDF files.
    
    Args:
        pdf_paths: List of paths to PDF files
        
    Returns:
        Dictionary mapping file paths to extracted content
    """
    results = {}
    
    for pdf_path in pdf_paths:
        try:
            logging.info(f"Extracting content from: {pdf_path}")
            content = extract_pdf_content(pdf_path)
            results[pdf_path] = content
        except Exception as e:
            logging.error(f"Error processing {pdf_path}: {str(e)}")
            results[pdf_path] = {
                "content": f"Error: {str(e)}",
                "metadata": []
            }
    
    return results


def cleanup_attachments(attachment_dir, older_than_days=7):
    """
    Clean up old attachment files from directory.
    
    Args:
        attachment_dir: Directory containing attachments
        older_than_days: Delete files older than this many days
        
    Returns:
        Number of files deleted
    """
    import time
    
    try:
        if not os.path.exists(attachment_dir):
            logging.info(f"Attachment directory does not exist: {attachment_dir}")
            return 0
        
        current_time = time.time()
        cutoff_time = current_time - (older_than_days * 24 * 60 * 60)
        deleted_count = 0
        
        for filename in os.listdir(attachment_dir):
            file_path = os.path.join(attachment_dir, filename)
            
            if not os.path.isfile(file_path):
                continue
            
            file_mtime = os.path.getmtime(file_path)
            
            if file_mtime < cutoff_time:
                try:
                    os.remove(file_path)
                    deleted_count += 1
                    logging.info(f"Deleted old attachment: {filename}")
                except Exception as e:
                    logging.error(f"Error deleting {filename}: {str(e)}")
        
        logging.info(f"Cleaned up {deleted_count} old attachment files from {attachment_dir}")
        return deleted_count
        
    except Exception as e:
        logging.error(f"Error during attachment cleanup: {str(e)}")
        return 0
    

def get_ai_response(prompt, agent_url="http://agentomatic:8000", conversation_history=None, stream=True, system_message=None,model=None):
    """Get AI response with conversation history context and optional system message"""
    try:
        logging.debug(f"Query received: {prompt}")
        OLLAMA_HOST= agent_url
        # Validate input
        if not prompt or not isinstance(prompt, str):
            return "Invalid input provided. Please enter a valid query."

        model_name = model if model is not None else Variable.get("LYNX_MODEL_NAME", default_var="Lynx:3.0-sonnet-4-5")
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'webshop-email-respond'})
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model {model_name}")

        # Build messages array with optional system message
        messages = []
        if system_message:
            messages.append({"role": "system", "content": system_message})
        
        if conversation_history:
            for history_item in conversation_history:
                messages.append({"role": "user", "content": history_item["prompt"]})
                messages.append({"role": "assistant", "content": history_item["response"]})
                
        # Add current prompt
        messages.append({"role": "user", "content": prompt})

        response = client.chat(
            model=model_name,  # <-- Fixed: Pass string directly
            messages=messages,
            stream=stream
        )
        logging.info(f"Raw response from agent: {str(response)[:500]}...")

        # Handle response based on streaming mode
        if stream:
            ai_content = ""
            for chunk in response:
                if hasattr(chunk, 'message') and hasattr(chunk.message, 'content'):
                    ai_content += chunk.message.content
                else:
                    logging.error("Chunk lacks expected 'message.content' structure")
                    return "Invalid response format from AI stream. Please try again later."
        else:
            if not (hasattr(response, 'message') and hasattr(response.message, 'content')):
                logging.error("Response lacks expected 'message.content' structure")
                return "Invalid response format from AI. Please try again later."
            ai_content = response.message.content

        logging.info(f"Full message content from agent: {ai_content[:500]}...")
        return ai_content.strip()

    except Exception as e:
        logging.error(f"Error in get_ai_response: {str(e)}")
        # raise f"An error occurred while processing your request: {str(e)}"
        return f"An error occurred while processing your request: {str(e)}"

import json
def extract_json_from_text(text):
    # Improved regex: Match a standalone JSON object (not nested in larger text)
    # This looks for { ... } that's not inside quotes or other braces
    pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
    matches = re.findall(pattern, text, re.DOTALL)
    
    for match in matches:  # Try each potential match
        try:
            parsed = json.loads(match)
            if isinstance(parsed, dict):  # Ensure it's an object, not array/primitive
                return parsed
        except json.JSONDecodeError as e:
            logging.debug(f"JSON parse failed on match '{match[:100]}...': {e}")
            continue
    
    logging.warning("No valid JSON object found in text.")
    return None