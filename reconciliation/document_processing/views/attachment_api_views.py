from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views import View
import logging
import tempfile
import os
from document_processing.utils.attachment_processor import SimplifiedAttachmentProcessor

logger = logging.getLogger(__name__)

@method_decorator(csrf_exempt, name='dispatch')
class ProcessItemWiseGRNAndAttachmentsAPI(View):
    """
    Upload ItemWiseGRN CSV/Excel file and automatically process all attachments
    """
    
    def post(self, request):
        """
        Upload ItemWiseGRN file and automatically process attachments
        
        POST data (multipart/form-data):
        - grn_file: CSV/Excel file containing ItemWiseGRN data
        - process_limit: Optional, max attachments to process (default: 10)
        - force_reprocess: Optional, reprocess even if already done (default: false)
        """
        try:
            # Check if file is provided
            if 'grn_file' not in request.FILES:
                return JsonResponse({
                    'success': False,
                    'error': 'No GRN file provided. Please upload a file with key "grn_file".',
                    'status': 'error'
                }, status=400)
            
            grn_file = request.FILES['grn_file']
            
            # Get optional parameters
            process_limit = int(request.POST.get('process_limit', 10))
            force_reprocess = request.POST.get('force_reprocess', 'false').lower() == 'true'
            
            # Validate file type
            allowed_extensions = ['.xlsx', '.xls', '.csv']
            file_extension = None
            for ext in allowed_extensions:
                if grn_file.name.lower().endswith(ext):
                    file_extension = ext
                    break
            
            if not file_extension:
                return JsonResponse({
                    'success': False,
                    'error': 'Invalid file type. Please upload an Excel (.xlsx, .xls) or CSV (.csv) file.',
                    'status': 'error'
                }, status=400)
            
            # Validate file size (50MB limit)
            max_size = 50 * 1024 * 1024  # 50MB
            if grn_file.size > max_size:
                return JsonResponse({
                    'success': False,
                    'error': f'File too large. Maximum size allowed is {max_size // (1024*1024)}MB.',
                    'status': 'error'
                }, status=400)
            
            # Process the uploaded file
            temp_path = None
            try:
                # Save file temporarily
                with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
                    for chunk in grn_file.chunks():
                        temp_file.write(chunk)
                    temp_path = temp_file.name
                
                logger.info(f"Processing ItemWiseGRN file for attachments: {grn_file.name}")
                
                # Use the enhanced attachment processor with new method
                processor = SimplifiedAttachmentProcessor()
                
                # Call the new method that processes directly from Excel
                results = processor.process_from_excel_file(
                    file_path=temp_path,
                    file_extension=file_extension,
                    process_limit=process_limit,
                    force_reprocess=force_reprocess
                )
                
                if not results['success']:
                    return JsonResponse({
                        'success': False,
                        'error': results['error'],
                        'status': 'processing_error'
                    }, status=400)
                
                # Return comprehensive results
                return JsonResponse({
                    'success': True,
                    'message': f'Processed {results["processed_attachments"]} attachments from uploaded file',
                    'status': 'completed',
                    'data': {
                        'file_info': {
                            'filename': grn_file.name,
                            'file_size': grn_file.size,
                            'total_attachments_found': results['total_attachments_found'],
                            'process_limit_applied': process_limit,
                            'force_reprocess': force_reprocess
                        },
                        'processing_summary': {
                            'attachments_processed': results['processed_attachments'],
                            'successful_extractions': results['successful_extractions'],
                            'failed_extractions': results['failed_extractions'],
                            'success_rate': results['success_rate']
                        },
                        'attachment_results': results['results']
                    }
                }, status=200)
                
            finally:
                # Clean up temporary file
                if temp_path and os.path.exists(temp_path):
                    os.unlink(temp_path)
            
        except Exception as e:
            logger.error(f"Error in ProcessItemWiseGRNAndAttachmentsAPI: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': f'Failed to process file: {str(e)}',
                'status': 'processing_error'
            }, status=500)
