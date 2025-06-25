import logging
from typing import Dict, Any
from django.db import transaction
from decimal import Decimal, InvalidOperation
from datetime import datetime

from document_processing.models import ItemWiseGrn, InvoiceData
from document_processing.utils.file_classifier import SmartFileClassifier  # Import the separate classifier
from document_processing.utils.processors.invoice_processors.invoice_pdf_processor import InvoicePDFProcessor

# OCR processor commented out for now
# from document_processing.utils.processors.invoice_processors.invoice_image_ocr_processor import InvoiceOCRProcessor

logger = logging.getLogger(__name__)

class SimplifiedAttachmentProcessor:
    """
    Simplified processor for API usage - TEXT PDFs ONLY
    Uses separate file_classifier.py for file classification
    """
    
    def __init__(self):
        # Use the separate file classifier
        self.file_classifier = SmartFileClassifier()
        self.text_pdf_processor = InvoicePDFProcessor()
        
        # OCR processor commented out for now
        # self.ocr_processor = InvoiceOCRProcessor()
        
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.errors = []
    
    def process_single_grn(self, grn_id: int) -> Dict[str, Any]:
        """Process attachments for a single GRN record"""
        try:
            grn_record = ItemWiseGrn.objects.get(id=grn_id)
            return self._process_grn_attachments(grn_record)
            
        except ItemWiseGrn.DoesNotExist:
            raise ValueError(f"GRN record with ID {grn_id} not found")
    
    def process_multiple_grns(self, limit: int = 50) -> Dict[str, Any]:
        """Process multiple GRN records"""
        from django.db import models
        
        # Get GRNs with attachments that haven't been processed yet
        processed_grn_ids = InvoiceData.objects.values_list('source_grn_id', flat=True).distinct()
        
        grn_records = ItemWiseGrn.objects.filter(
            models.Q(attachment_1__isnull=False) |
            models.Q(attachment_2__isnull=False) |
            models.Q(attachment_3__isnull=False) |
            models.Q(attachment_4__isnull=False) |
            models.Q(attachment_5__isnull=False)
        ).exclude(
            id__in=processed_grn_ids
        ).order_by('id')[:limit]
        
        results = []
        
        for grn_record in grn_records:
            try:
                result = self._process_grn_attachments(grn_record)
                results.append(result)
                
                # Log progress
                logger.info(f"Processed GRN {grn_record.id}: {result['successful_attachments']}/{result['total_attachments']} successful")
                
            except Exception as e:
                logger.error(f"Error processing GRN {grn_record.id}: {str(e)}")
                results.append({
                    'grn_id': grn_record.id,
                    'success': False,
                    'error': str(e),
                    'total_attachments': 0,
                    'successful_attachments': 0
                })
        
        return {
            'total_processed': len(results),
            'results': results
        }
    
    def _process_grn_attachments(self, grn_record: ItemWiseGrn) -> Dict[str, Any]:
        """Process all attachments for a single GRN record"""
        results = []
        
        # Check each attachment field
        for i in range(1, 6):
            attachment_url = getattr(grn_record, f'attachment_{i}')
            
            if attachment_url:
                try:
                    logger.info(f"Processing attachment {i} for GRN {grn_record.id}: {attachment_url}")
                    
                    # Check if already processed
                    existing = InvoiceData.objects.filter(
                        source_grn_id=grn_record,
                        attachment_number=str(i)
                    ).first()
                    
                    if existing:
                        logger.info(f"Attachment {i} already processed, skipping")
                        results.append({
                            'attachment': str(i),
                            'success': True,
                            'status': 'already_processed',
                            'invoice_id': existing.id,
                            'vendor_name': existing.vendor_name,
                            'invoice_number': existing.invoice_number
                        })
                        continue
                    
                    # Process the attachment
                    result = self._process_single_attachment(grn_record, str(i), attachment_url)
                    results.append(result)
                    
                except Exception as e:
                    error_msg = f"Attachment {i}: {str(e)}"
                    logger.error(error_msg)
                    results.append({
                        'attachment': str(i), 
                        'success': False, 
                        'error': error_msg
                    })
        
        successful_results = [r for r in results if r['success']]
        
        return {
            'grn_id': grn_record.id,
            'total_attachments': len(results),
            'successful_attachments': len(successful_results),
            'results': results
        }
    
    def _process_single_attachment(self, grn_record: ItemWiseGrn, attachment_number: str, attachment_url: str) -> Dict[str, Any]:
        """Process a single attachment file - TEXT PDFs ONLY"""
        
        # Step 1: Use the separate file classifier to download and analyze
        classification = self.file_classifier.download_and_analyze(attachment_url)
        
        if not classification['success']:
            self._save_error_record(grn_record, attachment_number, attachment_url, 
                                  classification['error'], 'unknown', None)
            raise Exception(classification['error'])
        
        temp_file_path = classification['temp_file_path']
        
        try:
            # Step 2: Process based on file type - TEXT PDFs ONLY
            file_type = classification['file_type']
            
            if file_type == 'pdf_text':
                # Process text-based PDF using the existing processor
                extracted_data = self.text_pdf_processor.process_file_path(temp_file_path)
                
            elif file_type == 'pdf_image':
                # Skip image-based PDFs for now
                logger.warning(f"Skipping image-based PDF: {attachment_url}")
                self._save_error_record(
                    grn_record, attachment_number, attachment_url,
                    "Image-based PDF processing not enabled yet. This PDF contains scanned images and needs OCR.", 
                    'pdf_image', classification['original_extension']
                )
                raise Exception("Image-based PDF - OCR processing not enabled yet")
                
                # COMMENTED OUT - Future OCR implementation
                # extracted_data = self.ocr_processor.process_image_pdf(temp_file_path)
                
            elif file_type == 'image':
                # Skip image files for now
                logger.warning(f"Skipping image file: {attachment_url}")
                self._save_error_record(
                    grn_record, attachment_number, attachment_url,
                    "Image file processing not enabled yet. This file needs OCR processing.", 
                    'image', classification['original_extension']
                )
                raise Exception("Image file - OCR processing not enabled yet")
                
                # COMMENTED OUT - Future OCR implementation
                # extracted_data = self.ocr_processor.process_image_file(temp_file_path)
                
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            # Step 3: Save to database
            invoice_record = self._save_extracted_data(
                grn_record, attachment_number, attachment_url,
                file_type, classification['original_extension'], extracted_data
            )
            
            logger.info(f"Successfully processed attachment {attachment_number} for GRN {grn_record.id}")
            
            return {
                'attachment': attachment_number,
                'success': True,
                'file_type': file_type,
                'original_extension': classification['original_extension'],
                'file_size': classification['file_size'],
                'processing_method': classification['processing_method'],
                'invoice_id': invoice_record.id,
                'vendor_name': extracted_data.get('vendor_name', ''),
                'invoice_number': extracted_data.get('invoice_number', ''),
                'invoice_total': str(extracted_data.get('invoice_total_post_gst', ''))
            }
            
        except Exception as e:
            # Save error record
            self._save_error_record(
                grn_record, attachment_number, attachment_url,
                str(e), classification.get('file_type', 'unknown'),
                classification.get('original_extension')
            )
            raise
            
        finally:
            # Clean up temp file using the classifier's method
            if temp_file_path:
                self.file_classifier.cleanup_temp_file(temp_file_path)
    
    def _save_extracted_data(self, grn_record: ItemWiseGrn, attachment_number: str,
                           attachment_url: str, file_type: str, original_extension: str,
                           extracted_data: Dict[str, Any]) -> InvoiceData:
        """Save extracted data to InvoiceData model"""
        
        with transaction.atomic():
            invoice_data = InvoiceData(
                source_grn_id=grn_record,
                attachment_number=attachment_number,
                attachment_url=attachment_url,
                file_type=file_type,
                original_file_extension=original_extension,
                
                # Basic info (extracted from invoice)
                vendor_name=extracted_data.get('vendor_name', ''),
                vendor_pan=extracted_data.get('vendor_pan', ''),
                vendor_gst=extracted_data.get('vendor_gst', ''),
                invoice_number=extracted_data.get('invoice_number', ''),
                # PO number will be auto-populated from GRN in model's save() method
                
                processing_status='completed',
                extracted_at=datetime.now()
            )
            
            # Parse date
            if extracted_data.get('invoice_date'):
                try:
                    date_str = extracted_data['invoice_date']
                    if '/' in date_str:
                        invoice_data.invoice_date = datetime.strptime(date_str, '%d/%m/%Y').date()
                    elif '-' in date_str:
                        invoice_data.invoice_date = datetime.strptime(date_str, '%d-%m-%Y').date()
                except ValueError:
                    logger.warning(f"Could not parse date: {extracted_data['invoice_date']}")
            
            # Parse financial fields
            financial_fields = {
                'invoice_value_without_gst': extracted_data.get('invoice_value_without_gst'),
                'invoice_total_post_gst': extracted_data.get('invoice_total_post_gst')
            }
            
            gst_details = extracted_data.get('gst_details', {})
            financial_fields.update({
                'cgst_rate': gst_details.get('cgst_rate'),
                'cgst_amount': gst_details.get('cgst_amount'),
                'sgst_rate': gst_details.get('sgst_rate'),
                'sgst_amount': gst_details.get('sgst_amount'),
                'igst_rate': gst_details.get('igst_rate'),
                'igst_amount': gst_details.get('igst_amount'),
                'total_gst_amount': gst_details.get('total_gst_amount')
            })
            
            # Convert to Decimal
            for field, value in financial_fields.items():
                if value:
                    try:
                        clean_value = str(value).replace(',', '').replace('₹', '').strip()
                        if clean_value:
                            setattr(invoice_data, field, Decimal(clean_value))
                    except (InvalidOperation, ValueError):
                        logger.warning(f"Could not parse {field}: {value}")
            
            # Store items as JSON
            items = extracted_data.get('items', [])
            if items:
                invoice_data.items_data = items
            
            invoice_data.save()
            logger.info(f"Saved invoice data for GRN {grn_record.id}, attachment {attachment_number}")
            return invoice_data
    
    def _save_error_record(self, grn_record: ItemWiseGrn, attachment_number: str,
                          attachment_url: str, error_message: str, file_type: str, 
                          original_extension: str):
        """Save error record when processing fails"""
        try:
            with transaction.atomic():
                invoice_data = InvoiceData(
                    source_grn_id=grn_record,
                    attachment_number=attachment_number,
                    attachment_url=attachment_url,
                    file_type=file_type or 'unknown',
                    original_file_extension=original_extension,
                    processing_status='failed',
                    error_message=error_message,
                    extracted_at=datetime.now()
                )
                invoice_data.save()
                logger.info(f"Saved error record for GRN {grn_record.id}, attachment {attachment_number}")
                
        except Exception as e:
            logger.error(f"Error saving error record: {str(e)}")


