import os
import json
import tempfile
import PyPDF2
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from pydantic import BaseModel, Field, ValidationError
from typing import Optional, List

from .processors.document_classifier import DocumentClassifier
from .processors.document_type_checker import DocumentTypeChecker
from .processors.invoice_processors.invoice_pdf_processor import InvoicePdfProcessor


class DocumentProcessRequest(BaseModel):
    """Schema for document processing request"""
    file_name: str = Field(..., description="Name of the uploaded file")
    file_content: str = Field(..., description="Base64 encoded file content")


class DocumentProcessResponse(BaseModel):
    """Schema for document processing response"""
    status: str = Field(..., description="Status of the processing (success/error)")
    document_type: Optional[str] = Field(None, description="Type of document (pdf/image)")
    content_type: Optional[str] = Field(None, description="Content type (po/invoice/unknown)")
    data: Optional[dict] = Field(None, description="Extracted data from the document")
    error: Optional[str] = Field(None, description="Error message if processing failed")


@csrf_exempt
@require_http_methods(["POST"])
def process_document(request):
    """
    API endpoint to process uploaded documents.
    Accepts document, classifies it, and extracts data if applicable.
    """
    try:
        # Parse request body
        request_data = json.loads(request.body)
        
        # Validate request using Pydantic
        try:
            validated_request = DocumentProcessRequest(**request_data)
        except ValidationError as e:
            return JsonResponse(
                DocumentProcessResponse(
                    status="error",
                    error=f"Invalid request format: {str(e)}"
                ).model_dump(),
                status=400
            )
        
        # Save uploaded file to temporary location
        import base64
        file_content = base64.b64decode(validated_request.file_content)
        file_path = default_storage.save(
            f'temp/{validated_request.file_name}', 
            ContentFile(file_content)
        )
        file_path = default_storage.path(file_path)
        
        try:
            # Classify document (PDF or image)
            doc_type, is_processable = DocumentClassifier.classify_document(file_path)
            
            if not is_processable:
                return JsonResponse(
                    DocumentProcessResponse(
                        status="error",
                        document_type=doc_type,
                        error="Document is not processable. For PDFs, only text-based PDFs are supported."
                    ).model_dump(),
                    status=400
                )
            
            # Process only PDF documents for now (as per requirements)
            if doc_type == 'pdf':
                # Extract text from PDF
                with open(file_path, 'rb') as f:
                    pdf_reader = PyPDF2.PdfReader(f)
                    text = ""
                    for page in pdf_reader.pages:
                        text += page.extract_text() + "\n\n"
                
                # Determine document content type (PO or Invoice)
                content_type = DocumentTypeChecker.determine_document_type(text)
                
                # Process based on content type
                if content_type == 'invoice':
                    # Process invoice
                    processor = InvoicePdfProcessor()
                    invoice_data = processor.process_pdf(file_path)
                    
                    return JsonResponse(
                        DocumentProcessResponse(
                            status="success",
                            document_type=doc_type,
                            content_type=content_type,
                            data=invoice_data.model_dump()
                        ).model_dump()
                    )
                elif content_type == 'po':
                    # For now, just return the document type
                    # In a real implementation, you would add PO processor here
                    return JsonResponse(
                        DocumentProcessResponse(
                            status="success",
                            document_type=doc_type,
                            content_type=content_type,
                            data={"message": "PO processing not implemented yet"}
                        ).model_dump()
                    )
                else:
                    return JsonResponse(
                        DocumentProcessResponse(
                            status="error",
                            document_type=doc_type,
                            content_type="unknown",
                            error="Unable to determine if document is PO or Invoice"
                        ).model_dump(),
                        status=400
                    )
            else:
                return JsonResponse(
                    DocumentProcessResponse(
                        status="error",
                        document_type=doc_type,
                        error="Only PDF processing is currently supported"
                    ).model_dump(),
                    status=400
                )
                
        finally:
            # Clean up temporary file
            if os.path.exists(file_path):
                os.remove(file_path)
                
    except Exception as e:
        return JsonResponse(
            DocumentProcessResponse(
                status="error",
                error=f"Error processing document: {str(e)}"
            ).model_dump(),
            status=500
        )
