# reconciliation/document_processing/views.py

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.utils.decorators import method_decorator
from django.views import View
import json
import logging
from .processors.invoice_processors.invoice_pdf_processor import InvoicePDFProcessor

logger = logging.getLogger(__name__)

@method_decorator(csrf_exempt, name='dispatch')
class ProcessInvoiceAPI(View):
    """
    API endpoint to process invoice PDF files
    """
    
    def post(self, request):
        """
        Process uploaded PDF invoice file
        
        Expected: multipart/form-data with 'pdf_file' field
        Returns: JSON response with extracted invoice data
        """
        try:
            # Check if file is provided
            if 'pdf_file' not in request.FILES:
                return JsonResponse({
                    'success': False,
                    'error': 'No PDF file provided. Please upload a file with key "pdf_file".',
                    'status': 'error'
                }, status=400)
            
            pdf_file = request.FILES['pdf_file']
            
            # Validate file type
            if not pdf_file.name.lower().endswith('.pdf'):
                return JsonResponse({
                    'success': False,
                    'error': 'Invalid file type. Please upload a PDF file.',
                    'status': 'error'
                }, status=400)
            
            # Validate file size (optional - adjust as needed)
            max_size = 10 * 1024 * 1024  # 10MB
            if pdf_file.size > max_size:
                return JsonResponse({
                    'success': False,
                    'error': f'File too large. Maximum size allowed is {max_size // (1024*1024)}MB.',
                    'status': 'error'
                }, status=400)
            
            # Initialize processor and process the file
            logger.info(f"Processing invoice file: {pdf_file.name}")
            processor = InvoicePDFProcessor()
            extracted_data = processor.process_uploaded_file(pdf_file)
            
            # Return successful response
            return JsonResponse({
                'success': True,
                'message': 'Invoice processed successfully',
                'status': 'completed',
                'data': extracted_data
            }, status=200)
            
        except ValueError as ve:
            logger.error(f"Validation error: {str(ve)}")
            return JsonResponse({
                'success': False,
                'error': str(ve),
                'status': 'validation_error'
            }, status=400)
            
        except Exception as e:
            logger.error(f"Error processing invoice: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': f'Failed to process invoice: {str(e)}',
                'status': 'processing_error'
            }, status=500)
    
    def get(self, request):
        """
        GET method to return API information
        """
        return JsonResponse({
            'message': 'Invoice Processing API',
            'version': '1.0',
            'endpoints': {
                'POST /api/process-invoice/': 'Upload and process PDF invoice file',
            },
            'usage': {
                'method': 'POST',
                'content_type': 'multipart/form-data',
                'required_fields': ['pdf_file'],
                'supported_formats': ['PDF'],
                'max_file_size': '10MB'
            }
        })


@csrf_exempt
@require_http_methods(["POST"])
def process_invoice_simple(request):
    """
    Simple function-based view for processing invoices
    Alternative endpoint with same functionality
    """
    try:
        # Check if file is provided
        if 'pdf_file' not in request.FILES:
            return JsonResponse({
                'success': False,
                'error': 'No PDF file provided'
            }, status=400)
        
        pdf_file = request.FILES['pdf_file']
        
        # Validate file
        if not pdf_file.name.lower().endswith('.pdf'):
            return JsonResponse({
                'success': False,
                'error': 'Invalid file type. Please upload a PDF file.'
            }, status=400)
        
        # Process the file
        processor = InvoicePDFProcessor()
        result = processor.process_uploaded_file(pdf_file)
        
        return JsonResponse({
            'success': True,
            'data': result,
            'message': 'Invoice processed successfully'
        })
        
    except Exception as e:
        logger.error(f"Error in process_invoice_simple: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@csrf_exempt
@require_http_methods(["GET"])
def health_check(request):
    """
    Health check endpoint to verify API is working
    """
    try:
        # Try to initialize processor to check if API key is configured
        processor = InvoicePDFProcessor()
        
        return JsonResponse({
            'status': 'healthy',
            'message': 'Invoice processing API is running',
            'model': processor.model_name,
            'timestamp': processor.invoice_schema.get('_metadata', {}).get('processed_at', 'N/A')
        })
        
    except Exception as e:
        return JsonResponse({
            'status': 'unhealthy',
            'error': str(e),
            'message': 'API configuration issue'
        }, status=500)


@csrf_exempt
@require_http_methods(["GET"])
def api_info(request):
    """
    API information and documentation endpoint
    """
    return JsonResponse({
        'api_name': 'Invoice Processing API',
        'version': '1.0.0',
        'description': 'Extract structured data from PDF invoices using AI',
        'endpoints': {
            'POST /api/process-invoice/': {
                'description': 'Upload and process PDF invoice (Class-based view)',
                'method': 'POST',
                'content_type': 'multipart/form-data',
                'parameters': {
                    'pdf_file': 'PDF file to process (required)'
                },
                'response': {
                    'success': 'boolean',
                    'data': 'extracted invoice data object',
                    'message': 'status message'
                }
            },
            'POST /api/process-invoice-simple/': {
                'description': 'Upload and process PDF invoice (Function-based view)',
                'method': 'POST',
                'content_type': 'multipart/form-data',
                'parameters': {
                    'pdf_file': 'PDF file to process (required)'
                }
            },
            'GET /api/health/': {
                'description': 'Health check endpoint',
                'method': 'GET'
            },
            'GET /api/info/': {
                'description': 'API information and documentation',
                'method': 'GET'
            }
        },
        'supported_formats': ['PDF'],
        'max_file_size': '10MB',
        'model_used': 'Google Gemini 1.5 Flash',
        'extracted_fields': [
            'invoice_number', 'invoice_date', 'due_date',
            'seller', 'buyer', 'items', 'taxes', 'totals',
            'payment_terms', 'bank_details'
        ]
    })

