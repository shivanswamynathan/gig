import os
import re
import PyPDF2
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import json
from dotenv import load_dotenv
from pydantic import BaseModel, Field, validator
from langchain_google_genai import GoogleGenerativeAI
from langchain.prompts import ChatPromptTemplate
from langchain.chains import LLMChain

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)


class InvoiceItem(BaseModel):
    """Schema for invoice line items"""
    description: str = Field(..., description="Description of the item")
    quantity: Optional[float] = Field(None, description="Quantity of the item")
    unit_price: Optional[float] = Field(None, description="Unit price of the item")
    total_amount: float = Field(..., description="Total amount for this line item")


class InvoiceData(BaseModel):
    """Schema for invoice data extracted from PDF"""
    invoice_number: str = Field(..., description="Invoice unique identifier")
    invoice_date: datetime = Field(..., description="Date the invoice was issued")
    due_date: Optional[datetime] = Field(None, description="Date payment is due")
    vendor_name: str = Field(..., description="Name of the vendor/supplier")
    vendor_address: Optional[str] = Field(None, description="Address of the vendor")
    vendor_tax_id: Optional[str] = Field(None, description="Tax ID or VAT number of vendor")
    client_name: str = Field(..., description="Name of the client/customer")
    client_address: Optional[str] = Field(None, description="Address of the client")
    currency: Optional[str] = Field(None, description="Currency used in the invoice")
    subtotal: Optional[float] = Field(None, description="Subtotal amount before tax")
    tax_amount: Optional[float] = Field(None, description="Total tax amount")
    total_amount: float = Field(..., description="Total amount including taxes")
    payment_terms: Optional[str] = Field(None, description="Payment terms")
    items: List[InvoiceItem] = Field(default_factory=list, description="Line items in the invoice")
    
    @validator('invoice_date', 'due_date', pre=True)
    def parse_dates(cls, value):
        """Parse dates from string format if needed"""
        if isinstance(value, str):
            # Try different date formats
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%m-%d-%Y']:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Date format not recognized: {value}")
        return value


class InvoicePdfProcessor:
    """
    Processes text-based PDF invoices to extract structured data 
    using Gemini LLM via LangChain.
    """
    
    def __init__(self):
        """Initialize the processor with Gemini LLM"""
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY environment variable not set")
        
        self.llm = GoogleGenerativeAI(
            model="gemini-1.5-pro",
            google_api_key=api_key,
            temperature=0.1,
            top_p=0.95
        )
        
        # Define prompt template for invoice extraction
        self.prompt_template = ChatPromptTemplate.from_template("""
        You are an expert invoice data extraction system. Extract the following information from the provided invoice text.
        Return a valid JSON object with the following fields:
        
        - invoice_number: The unique identifier for this invoice
        - invoice_date: The date the invoice was issued (YYYY-MM-DD format)
        - due_date: The date payment is due, if present (YYYY-MM-DD format)
        - vendor_name: Name of the company issuing the invoice
        - vendor_address: Full address of the vendor
        - vendor_tax_id: Tax ID or VAT number of the vendor, if present
        - client_name: Name of the client/customer
        - client_address: Full address of the client
        - currency: Currency used in the invoice
        - subtotal: Subtotal amount before taxes
        - tax_amount: Total tax amount
        - total_amount: Total amount including taxes
        - payment_terms: Payment terms if mentioned
        - items: Array of line items, each with:
          - description: Description of the item
          - quantity: Quantity of the item
          - unit_price: Unit price of the item
          - total_amount: Total amount for this line item
        
        For any fields not found in the invoice, use null.
        For dates, use YYYY-MM-DD format.
        For monetary values, use numbers without currency symbols.
        
        Invoice text:
        {invoice_text}
        
        JSON response:
        """)
        
        self.extraction_chain = LLMChain(llm=self.llm, prompt=self.prompt_template)
    
    def process_pdf(self, file_path: str) -> InvoiceData:
        """
        Process a PDF invoice and extract structured data.
        
        Args:
            file_path: Path to the PDF invoice file
            
        Returns:
            InvoiceData object containing structured invoice information
        """
        try:
            # Extract text from PDF
            text = self._extract_text_from_pdf(file_path)
            
            # Use LLM to extract structured data
            result = self.extraction_chain.invoke({"invoice_text": text})
            
            # Parse the JSON response
            extracted_data = json.loads(result["text"])
            
            # Validate and convert to Pydantic model
            invoice_data = InvoiceData(**extracted_data)
            
            return invoice_data
            
        except Exception as e:
            logger.error(f"Error processing invoice PDF: {str(e)}")
            raise
    
    def _extract_text_from_pdf(self, file_path: str) -> str:
        """
        Extract text from a PDF file.
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            Extracted text from the PDF
        """
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n\n"
                    
                return text
                
        except Exception as e:
            logger.error(f"Error extracting text from PDF: {str(e)}")
            raise