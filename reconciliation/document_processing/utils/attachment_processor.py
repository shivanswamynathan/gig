import logging
import pandas as pd
from typing import Dict, Any, List
from django.db import transaction
from decimal import Decimal, InvalidOperation
from datetime import datetime

from document_processing.models import ItemWiseGrn, InvoiceData , InvoiceItemData
from document_processing.utils.file_classifier import SmartFileClassifier
from document_processing.utils.processors.invoice_processors.invoice_pdf_processor import InvoicePDFProcessor


logger = logging.getLogger(__name__)

class SimplifiedAttachmentProcessor:
    """
    UPDATED: Process attachments directly from Excel file without database GRN lookup
    """
    
    def __init__(self):
        # Use the separate file classifier
        self.file_classifier = SmartFileClassifier()
        self.text_pdf_processor = InvoicePDFProcessor()
        
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.errors = []
    
    def process_from_excel_file(self, file_path: str, file_extension: str, process_limit: int = 10, force_reprocess: bool = False) -> Dict[str, Any]:
        """
        Process attachments directly from Excel file
        
        Args:
            file_path: Path to the Excel/CSV file
            file_extension: File extension (.xlsx, .xls, .csv)
            process_limit: Maximum number of attachments to process
            force_reprocess: Whether to reprocess already processed attachments
            
        Returns:
            Dictionary with processing results
        """
        try:
            
            attachment_data = self._extract_attachments_from_file(file_path, file_extension)
            
            if not attachment_data:
                return {
                    'success': False,
                    'error': 'No attachment URLs found in the uploaded file.',
                    'total_attachments_found': 0,
                    'processed_attachments': 0,
                    'successful_extractions': 0,
                    'failed_extractions': 0,
                    'results': []
                }
            
            # Step 2: Process attachments directly (no database lookup needed!)
            # Limit processing if requested
            attachments_to_process = attachment_data[:process_limit]
            
            processing_results = []
            successful_extractions = 0
            failed_extractions = 0
            
            for attachment_info in attachments_to_process:
                try:
                    logger.info(f"Processing attachment: {attachment_info['url'][:50]}...")
                    
                    # Check if already processed (unless force_reprocess=true)
                    if not force_reprocess:
                        existing = InvoiceData.objects.filter(attachment_url=attachment_info['url']).first()
                        if existing:
                            logger.info(f"Attachment already processed, skipping: {attachment_info['url']}")
                            processing_results.append({
                                'url': attachment_info['url'][:50] + '...',
                                'po_number': attachment_info['po_number'],
                                'grn_number': attachment_info.get('grn_number', 'N/A'),
                                'supplier': attachment_info.get('supplier', 'N/A'),
                                'success': True,
                                'status': 'already_processed',
                                'invoice_id': existing.id,
                                'vendor_name': existing.vendor_name,
                                'invoice_number': existing.invoice_number
                            })
                            successful_extractions += 1  # Count as success since data exists
                            continue
                    
                    # Process this attachment
                    result = self._process_attachment_direct(attachment_info)
                    processing_results.append(result)
                    
                    if result['success']:
                        successful_extractions += 1
                    else:
                        failed_extractions += 1
                    
                except Exception as e:
                    logger.error(f"Error processing attachment {attachment_info['url']}: {str(e)}")
                    processing_results.append({
                        'url': attachment_info['url'][:50] + '...',
                        'po_number': attachment_info['po_number'],
                        'success': False,
                        'error': str(e)
                    })
                    failed_extractions += 1
            
            return {
                'success': True,
                'total_attachments_found': len(attachment_data),
                'processed_attachments': len(attachments_to_process),
                'successful_extractions': successful_extractions,
                'failed_extractions': failed_extractions,
                'success_rate': f"{successful_extractions}/{len(attachments_to_process)}",
                'results': processing_results
            }
            
        except Exception as e:
            logger.error(f"Error processing Excel file: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'total_attachments_found': 0,
                'processed_attachments': 0,
                'successful_extractions': 0,
                'failed_extractions': 0,
                'results': []
            }
    
    def _extract_attachments_from_file(self, file_path: str, file_extension: str) -> List[Dict[str, Any]]:
        """
        Extract ALL attachment URLs directly from uploaded file
        
        Returns:
            List of dictionaries: [
                {
                    'url': 'https://example.com/invoice1.pdf',
                    'po_number': 'PO-2024-001',
                    'grn_number': 'GRN-001',
                    'supplier': 'ABC Corp',
                    'attachment_number': 1,
                    'row_number': 5
                },
                ...
            ]
        """
        try:
            # Read file into pandas DataFrame
            if file_extension in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path, header=0)
            else:  # CSV
                # Try different encodings
                encodings = ['utf-8', 'latin-1', 'cp1252']
                df = None
                for encoding in encodings:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                
                if df is None:
                    raise Exception("Could not read CSV file with any supported encoding")
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            logger.info(f"File loaded: {len(df)} rows, columns: {list(df.columns)}")
            
            # Normalize column names (case-insensitive mapping)
            column_mapping = {
                'po no.': 'po_no',
                'po no': 'po_no',
                'po number': 'po_no',
                'grn no.': 'grn_no',
                'grn no': 'grn_no',
                'grn number': 'grn_no',
                'supplier': 'supplier',
                'vendor': 'supplier',
                'attachment-1': 'attachment_1',
                'attachment-2': 'attachment_2',
                'attachment-3': 'attachment_3',
                'attachment-4': 'attachment_4',
                'attachment-5': 'attachment_5',
            }
            
            # Create case-insensitive column mapping
            normalized_columns = {}
            for col in df.columns:
                col_lower = col.lower().strip()
                if col_lower in column_mapping:
                    normalized_columns[col] = column_mapping[col_lower]
            
            if not normalized_columns:
                logger.warning(f"No matching columns found. Available columns: {list(df.columns)}")
                return []
            
            # Rename columns
            df = df.rename(columns=normalized_columns)
            
            # Extract ALL attachment URLs directly
            all_attachments = []
            
            for row_idx, row in df.iterrows():
                po_no = row.get('po_no')
                grn_no = row.get('grn_no', 'N/A')
                supplier = row.get('supplier', 'Unknown')
                
                if pd.isna(po_no) or not po_no:
                    continue
                
                po_no = str(po_no).strip()
                grn_no = str(grn_no).strip() if pd.notna(grn_no) else 'N/A'
                supplier = str(supplier).strip() if pd.notna(supplier) else 'Unknown'
                
                # Extract attachment URLs from this row
                for i in range(1, 6):
                    attachment_col = f'attachment_{i}'
                    if attachment_col in row:
                        url = row[attachment_col]
                        if pd.notna(url) and url and str(url).strip():
                            clean_url = str(url).strip()
                            if clean_url.startswith(('http://', 'https://')):
                                all_attachments.append({
                                    'url': clean_url,
                                    'po_number': po_no,
                                    'grn_number': grn_no,
                                    'supplier': supplier,
                                    'attachment_number': i,
                                    'row_number': row_idx + 1
                                })
            
            # Remove duplicates based on URL
            unique_attachments = []
            seen_urls = set()
            
            for attachment in all_attachments:
                if attachment['url'] not in seen_urls:
                    unique_attachments.append(attachment)
                    seen_urls.add(attachment['url'])
            
            logger.info(f"Extracted {len(unique_attachments)} unique attachments from {len(all_attachments)} total")
            
            return unique_attachments
            
        except Exception as e:
            logger.error(f"Error extracting attachments from file: {str(e)}")
            raise Exception(f"Failed to extract data from file: {str(e)}")
    
    def _process_attachment_direct(self, attachment_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single attachment directly from Excel data
        
        Args:
            attachment_info: {
                'url': 'https://...',
                'po_number': 'PO-001',
                'grn_number': 'GRN-001',
                'supplier': 'ABC Corp',
                'attachment_number': 1,
                'row_number': 5
            }
        
        Returns:
            Dictionary with processing results
        """
        url = attachment_info['url']
        
        try:
            # Step 1: Download and classify file
            classification = self.file_classifier.download_and_analyze(url)
            
            if not classification['success']:
                self._save_error_record_direct(attachment_info, classification['error'], 'unknown', None)
                return {
                    'url': url[:50] + '...',
                    'po_number': attachment_info['po_number'],
                    'grn_number': attachment_info['grn_number'],
                    'supplier': attachment_info['supplier'],
                    'success': False,
                    'error': classification['error'],
                    'file_type': 'unknown'
                }
            
            temp_file_path = classification['temp_file_path']
            
            try:
                # Step 2: Process based on file type
                file_type = classification['file_type']
                
                if file_type == 'pdf_text':
                    # Process text-based PDF
                    extracted_data = self.text_pdf_processor.process_file_path(temp_file_path)
                    
                elif file_type == 'pdf_image':
                    self._save_error_record_direct(
                        attachment_info,
                        "Image-based PDF processing not enabled yet. This PDF contains scanned images and needs OCR.",
                        'pdf_image', classification['original_extension']
                    )
                    return {
                        'url': url[:50] + '...',
                        'po_number': attachment_info['po_number'],
                        'success': False,
                        'error': "Image-based PDF processing not enabled yet",
                        'file_type': 'pdf_image'
                    }
                    
                elif file_type == 'image':
                    self._save_error_record_direct(
                        attachment_info,
                        "Image file processing not enabled yet. This file needs OCR processing.",
                        'image', classification['original_extension']
                    )
                    return {
                        'url': url[:50] + '...',
                        'po_number': attachment_info['po_number'],
                        'success': False,
                        'error': "Image file processing not enabled yet",
                        'file_type': 'image'
                    }
                else:
                    raise ValueError(f"Unsupported file type: {file_type}")
                
                # Step 3: Save to database
                invoice_record = self._save_extracted_data_direct(attachment_info, classification, extracted_data)
                
                return {
                    'url': url[:50] + '...',
                    'po_number': attachment_info['po_number'],
                    'grn_number': attachment_info['grn_number'],
                    'supplier': attachment_info['supplier'],
                    'success': True,
                    'file_type': file_type,
                    'processing_method': classification['processing_method'],
                    'invoice_id': invoice_record.id,
                    'vendor_name': extracted_data.get('vendor_name', ''),
                    'invoice_number': extracted_data.get('invoice_number', ''),
                    'invoice_total': str(extracted_data.get('invoice_total_post_gst', ''))
                }
                
            finally:
                # Clean up temp file
                if temp_file_path:
                    self.file_classifier.cleanup_temp_file(temp_file_path)
        
        except Exception as e:
            # Save error record
            self._save_error_record_direct(attachment_info, str(e), 'unknown', None)
            logger.error(f"Error processing attachment {url}: {str(e)}")
            return {
                'url': url[:50] + '...',
                'po_number': attachment_info['po_number'],
                'success': False,
                'error': str(e)
            }
    
    def _save_extracted_data_direct(self, attachment_info: Dict[str, Any], classification: Dict[str, Any], extracted_data: Dict[str, Any]) -> InvoiceData:
        """
        Save extracted data directly (without GRN reference)
        """
        with transaction.atomic():
            invoice_data = InvoiceData(
                attachment_number=str(attachment_info['attachment_number']),
                attachment_url=attachment_info['url'],
                file_type=classification['file_type'],
                original_file_extension=classification['original_extension'],
                
                # Basic info from Excel
                po_number=attachment_info['po_number'],
                
                # Extracted invoice data
                vendor_name=extracted_data.get('vendor_name', ''),
                vendor_pan=extracted_data.get('vendor_pan', ''),
                vendor_gst=extracted_data.get('vendor_gst', ''),
                invoice_number=extracted_data.get('invoice_number', ''),
                
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
                        clean_value = str(value).replace(',', '').replace('₹', '').replace('%', '').strip()
                        if clean_value:
                            setattr(invoice_data, field, Decimal(clean_value))
                    except (InvalidOperation, ValueError):
                        logger.warning(f"Could not parse {field}: {value}")
            

            
            invoice_data.save()

            items = extracted_data.get('items', [])
            if items:
                self._create_invoice_items(invoice_data, items, attachment_info)

            logger.info(f"Saved invoice data for PO {attachment_info['po_number']}, attachment {attachment_info['attachment_number']}")
            return invoice_data
    
    def _create_invoice_items(self, invoice_data: InvoiceData, items: List[Dict[str, Any]], attachment_info: Dict[str, Any]):
        """
        Create separate InvoiceItemData records for each item
        """
        try:
            items_to_create = []
            
            for idx, item in enumerate(items, 1):
                item_record = InvoiceItemData(
                    invoice_data=invoice_data,
                    item_description=item.get('description', ''),
                    hsn_code=item.get('hsn_code', ''),
                    unit_of_measurement=item.get('unit_of_measurement', ''),
                    item_sequence=idx,
                    
                    # Reference fields for easy querying
                    po_number=attachment_info['po_number'],
                    invoice_number=invoice_data.invoice_number,
                    vendor_name=invoice_data.vendor_name
                )
                
                # Parse quantity
                quantity_str = item.get('quantity', '')
                if quantity_str:
                    try:
                        clean_qty = str(quantity_str).replace(',', '').strip()
                        if clean_qty:
                            item_record.quantity = Decimal(clean_qty)
                    except (InvalidOperation, ValueError):
                        logger.warning(f"Could not parse quantity: {quantity_str}")
                
                # Parse unit price (if available in item data)
                unit_price_str = item.get('unit_price', '')
                if unit_price_str:
                    try:
                        clean_price = str(unit_price_str).replace(',', '').replace('₹', '').strip()
                        if clean_price:
                            item_record.unit_price = Decimal(clean_price)
                    except (InvalidOperation, ValueError):
                        logger.warning(f"Could not parse unit price: {unit_price_str}")
                
                # Parse item-wise invoice value
                item_value_str = item.get('invoice_value_item_wise', '')
                if item_value_str:
                    try:
                        clean_value = str(item_value_str).replace(',', '').replace('₹', '').strip()
                        if clean_value:
                            item_record.invoice_value_item_wise = Decimal(clean_value)
                    except (InvalidOperation, ValueError):
                        logger.warning(f"Could not parse item value: {item_value_str}")
                
                # Parse tax details if available in item
                tax_fields = {
                    'cgst_rate': item.get('cgst_rate'),
                    'cgst_amount': item.get('cgst_amount'),
                    'sgst_rate': item.get('sgst_rate'),
                    'sgst_amount': item.get('sgst_amount'),
                    'igst_rate': item.get('igst_rate'),
                    'igst_amount': item.get('igst_amount'),
                    'total_tax_amount': item.get('total_tax_amount'),
                    'item_total_amount': item.get('item_total_amount')
                }
                
                for field, value in tax_fields.items():
                    if value:
                        try:
                            clean_value = str(value).replace(',', '').replace('₹', '').strip()
                            if clean_value:
                                setattr(item_record, field, Decimal(clean_value))
                        except (InvalidOperation, ValueError):
                            logger.warning(f"Could not parse {field} for item {idx}: {value}")
                
                items_to_create.append(item_record)
            
            # Bulk create all items
            if items_to_create:
                InvoiceItemData.objects.bulk_create(items_to_create)
                logger.info(f"Created {len(items_to_create)} item records for invoice {invoice_data.invoice_number}")
        
        except Exception as e:
            logger.error(f"Error creating invoice items: {str(e)}")
    
    def _save_error_record_direct(self, attachment_info: Dict[str, Any], error_message: str, file_type: str, original_extension: str):
        """Save error record when processing fails (direct mode)"""
        try:
            with transaction.atomic():
                invoice_data = InvoiceData(
                    attachment_number=str(attachment_info['attachment_number']),
                    attachment_url=attachment_info['url'],
                    file_type=file_type or 'unknown',
                    original_file_extension=original_extension,
                    po_number=attachment_info['po_number'],
                    processing_status='failed',
                    error_message=error_message,
                    extracted_at=datetime.now()
                )
                invoice_data.save()
                logger.info(f"Saved error record for PO {attachment_info['po_number']}, attachment {attachment_info['attachment_number']}")
                
        except Exception as e:
            logger.error(f"Error saving error record: {str(e)}")

    # KEEP EXISTING METHODS for backward compatibility
    def process_single_grn(self, grn_id: int) -> Dict[str, Any]:
        """Process attachments for a single GRN record (EXISTING METHOD)"""
        try:
            grn_record = ItemWiseGrn.objects.get(id=grn_id)
            return self._process_grn_attachments(grn_record)
            
        except ItemWiseGrn.DoesNotExist:
            raise ValueError(f"GRN record with ID {grn_id} not found")
    
    def _process_grn_attachments(self, grn_record: ItemWiseGrn) -> Dict[str, Any]:
        """Process all attachments for a single GRN record (EXISTING METHOD)"""
        results = []
        
        # Check each attachment field
        for i in range(1, 6):
            attachment_url = getattr(grn_record, f'attachment_{i}')
            
            if attachment_url:
                try:
                    logger.info(f"Processing attachment {i} for GRN {grn_record.id}: {attachment_url}")
                    
                    # Check if already processed
                    existing = InvoiceData.objects.filter(
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
                    
                    # Process the attachment (existing method)
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
        """Process a single attachment file (EXISTING METHOD)"""
        
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
                
            elif file_type == 'image':
                # Skip image files for now
                logger.warning(f"Skipping image file: {attachment_url}")
                self._save_error_record(
                    grn_record, attachment_number, attachment_url,
                    "Image file processing not enabled yet. This file needs OCR processing.", 
                    'image', classification['original_extension']
                )
                raise Exception("Image file - OCR processing not enabled yet")
                
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            # Step 3: Save to database (existing method)
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
            # Save error record (existing method)
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
        """Save extracted data to InvoiceData model (EXISTING METHOD)"""
        
        with transaction.atomic():
            invoice_data = InvoiceData(
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
                        clean_value = str(value).replace(',', '').replace('₹', '').replace('%', '').strip()
                        if clean_value:
                            setattr(invoice_data, field, Decimal(clean_value))
                    except (InvalidOperation, ValueError):
                        logger.warning(f"Could not parse {field}: {value}")
            
            
            invoice_data.save()

            items = extracted_data.get('items', [])
            if items:
                # Create attachment info for the item creation method
                attachment_info = {
                    'po_number': grn_record.po_no,
                    'grn_number': grn_record.grn_no,
                    'supplier': grn_record.supplier,
                    'attachment_number': int(attachment_number)
                }
                self._create_invoice_items(invoice_data, items, attachment_info)
            logger.info(f"Saved invoice data for GRN {grn_record.id}, attachment {attachment_number}")
            return invoice_data
    
    def _save_error_record(self, grn_record: ItemWiseGrn, attachment_number: str,
                          attachment_url: str, error_message: str, file_type: str, 
                          original_extension: str):
        """Save error record when processing fails (EXISTING METHOD)"""
        try:
            with transaction.atomic():
                invoice_data = InvoiceData(
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
