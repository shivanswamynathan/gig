from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views import View
import json
import logging
from .processors.invoice_processors.invoice_pdf_processor import InvoicePDFProcessor
from .processors.data_ingestion.po_grn_extractor import POGRNExtractor

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
            
            # Validate file size
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


@method_decorator(csrf_exempt, name='dispatch')
class ProcessPOGRNAPI(View):
    """
    API endpoint to process PO/GRN Excel/CSV files
    """
    
    def post(self, request):
        """
        Process uploaded Excel/CSV PO/GRN file
        
        Expected: multipart/form-data with 'data_file' field
        Returns: JSON response with extracted PO/GRN data
        """
        try:
            # Check if file is provided
            if 'data_file' not in request.FILES:
                return JsonResponse({
                    'success': False,
                    'error': 'No data file provided. Please upload a file with key "data_file".',
                    'status': 'error'
                }, status=400)
            
            data_file = request.FILES['data_file']
            
            # Validate file type
            allowed_extensions = ['.xlsx', '.xls', '.csv']
            file_extension = '.' + data_file.name.lower().split('.')[-1]
            
            if file_extension not in allowed_extensions:
                return JsonResponse({
                    'success': False,
                    'error': f'Invalid file type. Please upload Excel (.xlsx, .xls) or CSV (.csv) file.',
                    'status': 'error'
                }, status=400)
            
            # Validate file size
            max_size = 50 * 1024 * 1024  # 50MB for data files
            if data_file.size > max_size:
                return JsonResponse({
                    'success': False,
                    'error': f'File too large. Maximum size allowed is {max_size // (1024*1024)}MB.',
                    'status': 'error'
                }, status=400)
            
            # Initialize processor and process the file
            logger.info(f"Processing PO/GRN file: {data_file.name}")
            extractor = POGRNExtractor()
            extracted_data = extractor.process_uploaded_file(data_file)
            
            # Return successful response
            return JsonResponse({
                'success': True,
                'message': 'PO/GRN data processed successfully',
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
            logger.error(f"Error processing PO/GRN file: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': f'Failed to process PO/GRN file: {str(e)}',
                'status': 'processing_error'
            }, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class BulkProcessAPI(View):
    """
    API endpoint to bulk process multiple data files
    """
    
    def post(self, request):
        """
        Bulk process multiple data files
        
        Expected: multipart/form-data with 'files' field (multiple files)
        Returns: JSON response with processing results for all files
        """
        try:
            # Get all uploaded files
            uploaded_files = request.FILES.getlist('files')
            
            if not uploaded_files:
                return JsonResponse({
                    'success': False,
                    'error': 'No files provided. Please upload files with key "files".',
                    'status': 'error'
                }, status=400)
            
            results = []
            extractor = POGRNExtractor()
            invoice_processor = InvoicePDFProcessor()
            
            for file in uploaded_files:
                try:
                    # Determine file type and process accordingly
                    if file.name.lower().endswith('.pdf'):
                        # Process as invoice
                        logger.info(f"Processing PDF invoice: {file.name}")
                        result = invoice_processor.process_uploaded_file(file)
                        results.append({
                            'filename': file.name,
                            'file_type': 'pdf_invoice',
                            'success': True,
                            'data': result
                        })
                    elif file.name.lower().endswith(('.xlsx', '.xls', '.csv')):
                        # Process as PO/GRN data
                        logger.info(f"Processing data file: {file.name}")
                        result = extractor.process_uploaded_file(file)
                        results.append({
                            'filename': file.name,
                            'file_type': 'po_grn_data',
                            'success': True,
                            'data': result
                        })
                    else:
                        # Unsupported file type
                        results.append({
                            'filename': file.name,
                            'file_type': 'unsupported',
                            'success': False,
                            'error': 'Unsupported file type. Only PDF, Excel, and CSV files are supported.'
                        })
                        
                except Exception as e:
                    logger.error(f"Error processing file {file.name}: {str(e)}")
                    results.append({
                        'filename': file.name,
                        'success': False,
                        'error': str(e)
                    })
            
            # Calculate summary
            successful = sum(1 for r in results if r['success'])
            failed = len(results) - successful
            
            return JsonResponse({
                'success': True,
                'message': f'Processed {len(results)} files: {successful} successful, {failed} failed',
                'status': 'completed',
                'summary': {
                    'total_files': len(results),
                    'successful': successful,
                    'failed': failed
                },
                'results': results
            }, status=200)
            
        except Exception as e:
            logger.error(f"Error in bulk processing: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': f'Bulk processing failed: {str(e)}',
                'status': 'processing_error'
            }, status=500)