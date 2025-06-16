import os
import PyPDF2
from PIL import Image
import io
from typing import Tuple, Optional


class DocumentClassifier:
    """
    Classifies uploaded documents as PDF or image.
    Only processes text-based PDFs (with extractable text).
    """
    
    VALID_PDF_EXTENSIONS = ['.pdf']
    VALID_IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.tiff', '.bmp']
    
    @staticmethod
    def classify_document(file_path: str) -> Tuple[str, bool]:
        """
        Classifies a document as PDF or image and checks if it's processable.
        
        Args:
            file_path: Path to the document file
            
        Returns:
            Tuple containing document type ('pdf' or 'image') and 
            boolean indicating if the document is processable
        """
        file_ext = os.path.splitext(file_path)[1].lower()
        
        # Check if it's a PDF
        if file_ext in DocumentClassifier.VALID_PDF_EXTENSIONS:
            is_text_pdf = DocumentClassifier._is_text_based_pdf(file_path)
            return 'pdf', is_text_pdf
            
        # Check if it's an image
        elif file_ext in DocumentClassifier.VALID_IMAGE_EXTENSIONS:
            is_valid_image = DocumentClassifier._is_valid_image(file_path)
            return 'image', is_valid_image
            
        # Unsupported file type
        return 'unsupported', False
    
    @staticmethod
    def _is_text_based_pdf(file_path: str) -> bool:
        """
        Checks if a PDF file contains extractable text.
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            True if the PDF contains extractable text, False otherwise
        """
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                
                # PDF must have at least one page
                if len(pdf_reader.pages) == 0:
                    return False
                
                # Check if the first page has extractable text
                text = pdf_reader.pages[0].extract_text()
                
                # If text is empty or only whitespace, it's likely an image-based PDF
                if not text or text.isspace():
                    return False
                    
                return True
                
        except Exception:
            return False
    
    @staticmethod
    def _is_valid_image(file_path: str) -> bool:
        """
        Checks if an image file is valid and can be opened.
        
        Args:
            file_path: Path to the image file
            
        Returns:
            True if the image is valid, False otherwise
        """
        try:
            with Image.open(file_path) as img:
                # Verify the image can be read
                img.verify()
                return True
        except Exception:
            return False