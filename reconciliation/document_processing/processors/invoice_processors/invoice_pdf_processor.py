import fitz  # PyMuPDF
import json
import logging
import tempfile
import os
from typing import Dict, Any
from langchain_google_genai import GoogleGenerativeAI
from langchain.prompts import PromptTemplate
from django.conf import settings
from datetime import datetime

logger = logging.getLogger(__name__)

class InvoicePDFProcessor:
    """
    Invoice PDF processor using LangChain and Google Generative AI
    """
    
    def __init__(self):
        """Initialize the processor"""
        self.api_key = getattr(settings, 'GOOGLE_API_KEY', None) or os.getenv('GOOGLE_API_KEY')
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY must be set in Django settings or environment variables")
        
        self.model_name = getattr(settings, 'GEMINI_MODEL', 'gemini-1.5-flash')
        
        self.llm = GoogleGenerativeAI(
            model=self.model_name,
            google_api_key=self.api_key,
            temperature=0.1  
        )
        
        # Define the invoice schema
        self.invoice_schema = {
            "invoice_number": "",
            "invoice_date": "",
            "due_date": "",
            "seller": {
                "name": "",
                "gstin": "",
                "address": "",
                "phone": "",
                "email": ""
            },
            "buyer": {
                "name": "",
                "gstin": "",
                "address": "",
                "phone": "",
                "email": ""
            },
            "items": [
                {
                    "description": "",
                    "hsn_sac": "",
                    "quantity": "",
                    "unit": "",
                    "rate": "",
                    "discount": "",
                    "amount": ""
                }
            ],
            "subtotal": "",
            "discount_total": "",
            "taxes": {
                "cgst": "",
                "sgst": "",
                "igst": "",
                "cess": "",
                "total_tax": ""
            },
            "grand_total": "",
            "amount_in_words": "",
            "payment_terms": "",
            "bank_details": {
                "bank_name": "",
                "account_number": "",
                "ifsc_code": "",
                "branch": ""
            }
        }
    
    def extract_text_from_pdf(self, file_path: str) -> str:
        """
        Extract text from PDF using PyMuPDF
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            Extracted text as string
        """
        try:
            doc = fitz.open(file_path)
            text = ""
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                text += f"\n--- Page {page_num + 1} ---\n"
                text += page.get_text()
            
            doc.close()
            return text.strip()
            
        except Exception as e:
            logger.error(f"Error extracting text from PDF: {str(e)}")
            raise Exception(f"Failed to extract text from PDF: {str(e)}")
    
    def create_extraction_prompt(self) -> PromptTemplate:
        """
        Create prompt template for invoice extraction
        
        Returns:
            PromptTemplate for invoice extraction
        """
        template = """
You are an expert invoice data extraction system. Extract structured information from the following invoice text and return it as a valid JSON object.

EXTRACTION RULES:
1. Extract ALL available information from the invoice
2. If a field is not found or unclear, use an empty string ""
3. For numerical values, extract only the number (remove currency symbols like ₹, $, etc.)
4. For dates, use DD/MM/YYYY or DD-MM-YYYY format
5. For GST numbers, extract the complete 15-digit alphanumeric code
6. For items array, include ALL line items found in the invoice
7. Be precise and accurate - double-check all extracted values
8. Return ONLY the JSON object, no additional text

REQUIRED JSON STRUCTURE:
{schema}

INVOICE TEXT TO PROCESS:
{invoice_text}

Extract the information and return the JSON object:
"""
        
        return PromptTemplate(
            input_variables=["schema", "invoice_text"],
            template=template
        )
    
    def validate_and_clean_json(self, json_str: str) -> Dict[str, Any]:
        """
        Validate and clean the extracted JSON response
        
        Args:
            json_str: Raw JSON string from LLM
            
        Returns:
            Cleaned and validated JSON dict
        """
        try:
            # Clean the response - remove markdown formatting if present
            json_str = json_str.strip()
            if json_str.startswith('```json'):
                json_str = json_str[7:]
            if json_str.endswith('```'):
                json_str = json_str[:-3]
            json_str = json_str.strip()
            
            # Parse the JSON
            extracted_data = json.loads(json_str)
            
            # Validate structure against schema
            validated_data = self.invoice_schema.copy()
            
            # Update validated_data with extracted_data, preserving structure
            for key, value in extracted_data.items():
                if key in validated_data:
                    if isinstance(validated_data[key], dict) and isinstance(value, dict):
                        # For nested dictionaries, update individual keys
                        for sub_key, sub_value in value.items():
                            if sub_key in validated_data[key]:
                                validated_data[key][sub_key] = sub_value
                    else:
                        validated_data[key] = value
            
            return validated_data
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {str(e)}")
            logger.error(f"Raw JSON string: {json_str[:500]}...")
            raise ValueError(f"Invalid JSON format returned by LLM: {str(e)}")
        except Exception as e:
            logger.error(f"Error validating JSON: {str(e)}")
            raise
    
    def process_uploaded_file(self, uploaded_file) -> Dict[str, Any]:
        """
        Process Django uploaded file and extract invoice data
        
        Args:
            uploaded_file: Django UploadedFile object
            
        Returns:
            Dictionary containing extracted invoice data
        """
        temp_path = None
        try:
            # Save uploaded file to temporary location
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                for chunk in uploaded_file.chunks():
                    temp_file.write(chunk)
                temp_path = temp_file.name
            
            # Extract text from PDF
            logger.info(f"Extracting text from uploaded file: {uploaded_file.name}")
            extracted_text = self.extract_text_from_pdf(temp_path)
            
            if not extracted_text:
                raise ValueError("No text could be extracted from the PDF file")
            
            # Create and format the prompt
            prompt_template = self.create_extraction_prompt()
            formatted_prompt = prompt_template.format(
                schema=json.dumps(self.invoice_schema, indent=2),
                invoice_text=extracted_text
            )
            
            # Get response from LLM
            logger.info("Processing invoice data with LLM...")
            llm_response = self.llm.invoke(formatted_prompt)
            
            # Validate and clean the JSON response
            extracted_data = self.validate_and_clean_json(llm_response)
            
            # Add processing metadata
            extracted_data["_metadata"] = {
                "filename": uploaded_file.name,
                "file_size": uploaded_file.size,
                "processed_at": datetime.now().isoformat(),
                "model_used": self.model_name,
                "text_length": len(extracted_text),
                "processing_status": "success"
            }
            
            logger.info(f"Successfully processed invoice: {uploaded_file.name}")
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error processing uploaded file {uploaded_file.name}: {str(e)}")
            
            # Return error information
            error_data = self.invoice_schema.copy()
            error_data["_metadata"] = {
                "filename": uploaded_file.name,
                "file_size": getattr(uploaded_file, 'size', 0),
                "processed_at": datetime.now().isoformat(),
                "processing_status": "failed",
                "error_message": str(e)
            }
            
            raise Exception(f"Failed to process invoice: {str(e)}")
            
        finally:
            # Clean up temporary file
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception as cleanup_error:
                    logger.warning(f"Failed to cleanup temp file: {cleanup_error}")
    
    def process_file_path(self, file_path: str) -> Dict[str, Any]:
        """
        Process invoice from file path (for testing purposes)
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            Dictionary containing extracted invoice data
        """
        try:
            # Extract text from PDF
            logger.info(f"Extracting text from file: {file_path}")
            extracted_text = self.extract_text_from_pdf(file_path)
            
            if not extracted_text:
                raise ValueError("No text could be extracted from the PDF file")
            
            # Create and format the prompt
            prompt_template = self.create_extraction_prompt()
            formatted_prompt = prompt_template.format(
                schema=json.dumps(self.invoice_schema, indent=2),
                invoice_text=extracted_text
            )
            
            # Get response from LLM
            logger.info("Processing invoice data with LLM...")
            llm_response = self.llm.invoke(formatted_prompt)
            
            # Validate and clean the JSON response
            extracted_data = self.validate_and_clean_json(llm_response)
            
            # Add processing metadata
            extracted_data["_metadata"] = {
                "file_path": file_path,
                "processed_at": datetime.now().isoformat(),
                "model_used": self.model_name,
                "text_length": len(extracted_text),
                "processing_status": "success"
            }
            
            logger.info(f"Successfully processed invoice from: {file_path}")
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            raise Exception(f"Failed to process invoice: {str(e)}")


