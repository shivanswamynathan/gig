import requests
import tempfile
import os
import logging
from PIL import Image
import fitz  # PyMuPDF
from typing import Tuple, Dict, Any

logger = logging.getLogger(__name__)

class SmartFileClassifier:
    """
    Smart file classifier that determines processing method needed
    """
    
    @staticmethod
    def download_and_analyze(url: str) -> Dict[str, Any]:
        """
        Download file and perform complete analysis
        
        Returns:
            {
                'success': bool,
                'temp_file_path': str,
                'original_extension': str,
                'detected_format': str,
                'file_type': str,  # pdf_text, pdf_image, image, unknown
                'processing_method': str,
                'file_size': int,
                'error': str or None
            }
        """
        result = {
            'success': False,
            'temp_file_path': None,
            'original_extension': None,
            'detected_format': None,
            'file_type': 'unknown',
            'processing_method': None,
            'file_size': 0,
            'error': None
        }
        
        try:
            # Step 1: Download file
            logger.info(f"Downloading file from: {url}")
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            result['file_size'] = len(response.content)
            
            # Step 2: Detect file format from content and headers
            content_type = response.headers.get('content-type', '').lower()
            first_bytes = response.content[:20]
            
            # File signature detection
            if first_bytes.startswith(b'%PDF'):
                detected_format = 'PDF'
                extension = '.pdf'
            elif first_bytes.startswith(b'\xff\xd8\xff'):
                detected_format = 'JPEG'
                extension = '.jpg'
            elif first_bytes.startswith(b'\x89PNG\r\n\x1a\n'):
                detected_format = 'PNG'
                extension = '.png'
            elif 'pdf' in content_type:
                detected_format = 'PDF'
                extension = '.pdf'
            elif any(img_type in content_type for img_type in ['jpeg', 'jpg']):
                detected_format = 'JPEG'
                extension = '.jpg'
            elif 'png' in content_type:
                detected_format = 'PNG'
                extension = '.png'
            else:
                # Fallback to URL extension
                url_lower = url.lower()
                if url_lower.endswith('.pdf'):
                    detected_format = 'PDF'
                    extension = '.pdf'
                elif url_lower.endswith(('.jpg', '.jpeg')):
                    detected_format = 'JPEG'
                    extension = '.jpg'
                elif url_lower.endswith('.png'):
                    detected_format = 'PNG'
                    extension = '.png'
                else:
                    raise ValueError(f"Unsupported file format: {url}")
            
            result['detected_format'] = detected_format
            result['original_extension'] = extension
            
            # Step 3: Save to temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=extension) as temp_file:
                temp_file.write(response.content)
                result['temp_file_path'] = temp_file.name
            
            # Step 4: Analyze content to determine processing method
            if detected_format == 'PDF':
                # Check if PDF has extractable text
                can_extract_text = SmartFileClassifier._analyze_pdf_content(result['temp_file_path'])
                
                if can_extract_text:
                    result['file_type'] = 'pdf_text'
                    result['processing_method'] = 'Direct text extraction + LLM'
                else:
                    result['file_type'] = 'pdf_image'
                    result['processing_method'] = 'PDF → Images → OCR → LLM'
                    
            elif detected_format in ['JPEG', 'PNG']:
                # Verify image is valid
                if SmartFileClassifier._verify_image(result['temp_file_path']):
                    result['file_type'] = 'image'
                    result['processing_method'] = 'OCR → LLM'
                else:
                    raise ValueError("Invalid image file")
            
            result['success'] = True
            logger.info(f"File classified as: {result['file_type']} ({result['detected_format']})")
            
        except Exception as e:
            error_msg = f"Error analyzing file {url}: {str(e)}"
            logger.error(error_msg)
            result['error'] = error_msg
            
            # Clean up temp file if created
            if result['temp_file_path'] and os.path.exists(result['temp_file_path']):
                try:
                    os.unlink(result['temp_file_path'])
                except:
                    pass
                result['temp_file_path'] = None
        
        return result
    
    @staticmethod
    def _analyze_pdf_content(pdf_path: str) -> bool:
        """
        Analyze PDF to determine if it has extractable text
        
        Returns:
            True if text can be extracted, False if OCR is needed
        """
        try:
            doc = fitz.open(pdf_path)
            total_text_length = 0
            pages_to_check = min(3, len(doc))  # Check first 3 pages
            
            for page_num in range(pages_to_check):
                page = doc[page_num]
                text = page.get_text().strip()
                total_text_length += len(text)
            
            doc.close()
            
            # Decision threshold: if average > 100 chars per page, consider it text-based
            avg_text_per_page = total_text_length / pages_to_check if pages_to_check > 0 else 0
            has_extractable_text = avg_text_per_page > 100
            
            logger.info(f"PDF analysis: {total_text_length} chars in {pages_to_check} pages, "
                       f"avg {avg_text_per_page:.1f} per page, text-extractable: {has_extractable_text}")
            
            return has_extractable_text
            
        except Exception as e:
            logger.error(f"Error analyzing PDF content: {str(e)}")
            return False  # Assume OCR needed if analysis fails
    
    @staticmethod
    def _verify_image(image_path: str) -> bool:
        """Verify that image file is valid and openable"""
        try:
            with Image.open(image_path) as img:
                img.verify()
            return True
        except Exception as e:
            logger.error(f"Image verification failed: {str(e)}")
            return False
    
    @staticmethod
    def cleanup_temp_file(temp_path: str):
        """Clean up temporary file"""
        try:
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)
                logger.info(f"Cleaned up temporary file: {temp_path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup temp file {temp_path}: {str(e)}")
