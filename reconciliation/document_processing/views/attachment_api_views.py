from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views import View
from django.core.paginator import Paginator
from django.db.models import Q, Count
import logging
import json
from document_processing.models import InvoiceData, ItemWiseGrn
from document_processing.utils.attachment_processor import SimplifiedAttachmentProcessor

logger = logging.getLogger(__name__)

@method_decorator(csrf_exempt, name='dispatch')
class ProcessTextPDFsAPI(View):
    """API to process text PDFs using PO Number"""
    
    def post(self, request):
        """
        Process attachments via API using PO Number
        
        POST data:
        {
            "po_number": "PO-2024-001",  // PO Number (much easier to use!)
            "limit": 10,                 // Optional: max GRNs to process (default: 1)
            "force_reprocess": false     // Optional: reprocess even if already done
        }
        """
        try:
            # Parse request body
            if request.body:
                body = json.loads(request.body)
            else:
                body = {}
            
            po_number = body.get('po_number')
            limit = body.get('limit', 1)
            force_reprocess = body.get('force_reprocess', False)
            
            processor = SimplifiedAttachmentProcessor()
            
            if po_number:
                # Process all GRNs for specific PO Number
                try:
                    # Find all GRNs for this PO that have attachments
                    grns_for_po = ItemWiseGrn.objects.filter(
                        po_no=po_number
                    ).filter(
                        Q(attachment_1__isnull=False) |
                        Q(attachment_2__isnull=False) |
                        Q(attachment_3__isnull=False) |
                        Q(attachment_4__isnull=False) |
                        Q(attachment_5__isnull=False)
                    )
                    
                    if not grns_for_po.exists():
                        return JsonResponse({
                            'success': False,
                            'error': f'No GRN records found for PO number {po_number} with attachments'
                        }, status=404)
                    
                    # Process each GRN for this PO
                    all_results = []
                    total_attachments = 0
                    total_successful = 0
                    
                    for grn in grns_for_po:
                        # Check if already processed (unless force_reprocess=true)
                        if not force_reprocess:
                            existing = InvoiceData.objects.filter(source_grn_id=grn).first()
                            if existing:
                                logger.info(f"GRN {grn.id} for PO {po_number} already processed, skipping")
                                continue
                        
                        # Process this GRN
                        try:
                            result = processor.process_single_grn(grn.id)
                            all_results.append(result)
                            total_attachments += result['total_attachments']
                            total_successful += result['successful_attachments']
                            
                        except Exception as e:
                            logger.error(f"Error processing GRN {grn.id} for PO {po_number}: {str(e)}")
                            all_results.append({
                                'grn_id': grn.id,
                                'success': False,
                                'error': str(e)
                            })
                    
                    return JsonResponse({
                        'success': True,
                        'message': f'Processed PO {po_number}',
                        'data': {
                            'po_number': po_number,
                            'total_grns_found': grns_for_po.count(),
                            'total_grns_processed': len(all_results),
                            'total_attachments': total_attachments,
                            'total_successful_extractions': total_successful,
                            'grn_results': all_results
                        }
                    })
                    
                except Exception as e:
                    return JsonResponse({
                        'success': False,
                        'error': f'Error processing PO {po_number}: {str(e)}'
                    }, status=500)
            
            else:
                # Process multiple POs (find unprocessed POs)
                
                # Get PO numbers that have attachments but haven't been processed
                processed_grn_ids = InvoiceData.objects.values_list('source_grn_id', flat=True).distinct()
                
                unprocessed_pos = ItemWiseGrn.objects.filter(
                    Q(attachment_1__isnull=False) |
                    Q(attachment_2__isnull=False) |
                    Q(attachment_3__isnull=False) |
                    Q(attachment_4__isnull=False) |
                    Q(attachment_5__isnull=False)
                ).exclude(
                    id__in=processed_grn_ids
                ).values_list('po_no', flat=True).distinct()[:limit]
                
                results = []
                
                for po_num in unprocessed_pos:
                    try:
                        # Process this PO
                        po_result = self._process_po_number(processor, po_num)
                        results.append(po_result)
                        
                    except Exception as e:
                        logger.error(f"Error processing PO {po_num}: {str(e)}")
                        results.append({
                            'po_number': po_num,
                            'success': False,
                            'error': str(e)
                        })
                
                return JsonResponse({
                    'success': True,
                    'message': f'Processed {len(results)} PO numbers',
                    'data': {
                        'total_pos_processed': len(results),
                        'po_results': results
                    }
                })
            
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'error': 'Invalid JSON in request body'
            }, status=400)
            
        except Exception as e:
            logger.error(f"Error in process API: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)
    
    def _process_po_number(self, processor, po_number):
        """Helper method to process all GRNs for a PO number"""
        grns_for_po = ItemWiseGrn.objects.filter(po_no=po_number).filter(
            Q(attachment_1__isnull=False) |
            Q(attachment_2__isnull=False) |
            Q(attachment_3__isnull=False) |
            Q(attachment_4__isnull=False) |
            Q(attachment_5__isnull=False)
        )
        
        all_results = []
        total_attachments = 0
        total_successful = 0
        
        for grn in grns_for_po:
            try:
                result = processor.process_single_grn(grn.id)
                all_results.append(result)
                total_attachments += result['total_attachments']
                total_successful += result['successful_attachments']
                
            except Exception as e:
                all_results.append({
                    'grn_id': grn.id,
                    'success': False,
                    'error': str(e)
                })
        
        return {
            'po_number': po_number,
            'success': len(all_results) > 0,
            'total_grns': len(all_results),
            'total_attachments': total_attachments,
            'total_successful_extractions': total_successful,
            'grn_results': all_results
        }

@method_decorator(csrf_exempt, name='dispatch')
class AttachmentStatusAPI(View):
    """API to check processing status using PO Number"""
    
    def get(self, request):
        """
        Get processing status using PO Number
        
        Query params:
        ?po_number=PO-2024-001   // Status for specific PO
        ?summary=true            // Get overall summary
        """
        try:
            po_number = request.GET.get('po_number')
            get_summary = request.GET.get('summary', 'false').lower() == 'true'
            
            if po_number:
                # Status for specific PO Number
                
                # Find all GRNs for this PO
                grns_for_po = ItemWiseGrn.objects.filter(po_no=po_number)
                
                if not grns_for_po.exists():
                    return JsonResponse({
                        'success': False,
                        'error': f'No GRN records found for PO number {po_number}'
                    }, status=404)
                
                po_summary = {
                    'po_number': po_number,
                    'total_grns': grns_for_po.count(),
                    'supplier': grns_for_po.first().supplier if grns_for_po.exists() else None,
                    'grns': []
                }
                
                total_attachments = 0
                total_processed = 0
                
                for grn in grns_for_po:
                    # Get attachments for this GRN
                    attachments = []
                    grn_attachment_count = 0
                    grn_processed_count = 0
                    
                    for i in range(1, 6):
                        url = getattr(grn, f'attachment_{i}')
                        if url:
                            grn_attachment_count += 1
                            total_attachments += 1
                            
                            # Check if processed
                            invoice_record = InvoiceData.objects.filter(
                                source_grn_id=grn,
                                attachment_number=str(i)
                            ).first()
                            
                            if invoice_record:
                                grn_processed_count += 1
                                total_processed += 1
                            
                            attachments.append({
                                'attachment_number': i,
                                'url': url[:50] + '...' if len(url) > 50 else url,
                                'processed': invoice_record is not None,
                                'status': invoice_record.processing_status if invoice_record else 'not_processed',
                                'file_type': invoice_record.file_type if invoice_record else None,
                                'vendor_name': invoice_record.vendor_name if invoice_record else None,
                                'invoice_number': invoice_record.invoice_number if invoice_record else None,
                                'error': invoice_record.error_message if invoice_record and invoice_record.processing_status == 'failed' else None
                            })
                    
                    if grn_attachment_count > 0:  # Only include GRNs with attachments
                        po_summary['grns'].append({
                            'grn_id': grn.id,
                            'grn_number': grn.grn_no,
                            'supplier': grn.supplier,
                            'total_attachments': grn_attachment_count,
                            'processed_attachments': grn_processed_count,
                            'fully_processed': grn_processed_count >= grn_attachment_count,
                            'attachments': attachments
                        })
                
                po_summary['total_attachments'] = total_attachments
                po_summary['total_processed_attachments'] = total_processed
                po_summary['processing_progress'] = f"{total_processed}/{total_attachments}"
                
                return JsonResponse({
                    'success': True,
                    'data': po_summary
                })
            
            elif get_summary:
                # Overall summary statistics (same as before)
                
                total_grns = ItemWiseGrn.objects.count()
                grns_with_attachments = ItemWiseGrn.objects.filter(
                    Q(attachment_1__isnull=False) |
                    Q(attachment_2__isnull=False) |
                    Q(attachment_3__isnull=False) |
                    Q(attachment_4__isnull=False) |
                    Q(attachment_5__isnull=False)
                ).count()
                
                # Count unique POs with attachments
                pos_with_attachments = ItemWiseGrn.objects.filter(
                    Q(attachment_1__isnull=False) |
                    Q(attachment_2__isnull=False) |
                    Q(attachment_3__isnull=False) |
                    Q(attachment_4__isnull=False) |
                    Q(attachment_5__isnull=False)
                ).values('po_no').distinct().count()
                
                processed_grns = InvoiceData.objects.values('source_grn_id').distinct().count()
                
                # Get processed PO count
                processed_pos = InvoiceData.objects.select_related('source_grn_id').values(
                    'source_grn_id__po_no'
                ).distinct().count()
                
                total_invoices = InvoiceData.objects.count()
                
                status_counts = {}
                for item in InvoiceData.objects.values('processing_status').annotate(count=Count('id')):
                    status_counts[item['processing_status']] = item['count']
                
                file_type_counts = {}
                for item in InvoiceData.objects.values('file_type').annotate(count=Count('id')):
                    file_type_counts[item['file_type']] = item['count']
                
                # Recent activity with PO numbers
                recent_success = list(InvoiceData.objects.filter(
                    processing_status='completed'
                ).select_related('source_grn_id').order_by('-extracted_at')[:5].values(
                    'source_grn_id__po_no', 'vendor_name', 'invoice_number', 'extracted_at'
                ))
                
                recent_failures = list(InvoiceData.objects.filter(
                    processing_status='failed'
                ).select_related('source_grn_id').order_by('-created_at')[:5].values(
                    'source_grn_id__po_no', 'error_message', 'created_at'
                ))
                
                return JsonResponse({
                    'success': True,
                    'data': {
                        'summary': {
                            'total_grns': total_grns,
                            'grns_with_attachments': grns_with_attachments,
                            'unique_pos_with_attachments': pos_with_attachments,
                            'processed_grns': processed_grns,
                            'processed_pos': processed_pos,
                            'processing_progress_grns': f"{processed_grns}/{grns_with_attachments}",
                            'processing_progress_pos': f"{processed_pos}/{pos_with_attachments}",
                            'total_invoices': total_invoices
                        },
                        'status_breakdown': status_counts,
                        'file_type_breakdown': file_type_counts,
                        'recent_success': recent_success,
                        'recent_failures': recent_failures
                    }
                })
            
            else:
                # List recent invoice records (same as before but show PO numbers)
                page = int(request.GET.get('page', 1))
                page_size = min(int(request.GET.get('page_size', 20)), 100)
                
                queryset = InvoiceData.objects.select_related('source_grn_id').order_by('-created_at')
                
                # Filters
                status = request.GET.get('status')
                if status:
                    queryset = queryset.filter(processing_status=status)
                
                file_type = request.GET.get('file_type')
                if file_type:
                    queryset = queryset.filter(file_type=file_type)
                
                # Filter by PO number
                po_filter = request.GET.get('po_number')
                if po_filter:
                    queryset = queryset.filter(source_grn_id__po_no__icontains=po_filter)
                
                # Paginate
                paginator = Paginator(queryset, page_size)
                page_obj = paginator.get_page(page)
                
                # Serialize
                results = []
                for record in page_obj:
                    results.append({
                        'id': record.id,
                        'grn_id': record.source_grn_id.id,
                        'po_number': record.source_grn_id.po_no,  # Show PO number
                        'attachment_number': record.attachment_number,
                        'file_type': record.file_type,
                        'processing_status': record.processing_status,
                        'vendor_name': record.vendor_name,
                        'invoice_number': record.invoice_number,
                        'invoice_total': str(record.invoice_total_post_gst) if record.invoice_total_post_gst else None,
                        'created_at': record.created_at.isoformat(),
                        'extracted_at': record.extracted_at.isoformat() if record.extracted_at else None,
                        'error_message': record.error_message if record.processing_status == 'failed' else None
                    })
                
                return JsonResponse({
                    'success': True,
                    'data': {
                        'results': results,
                        'pagination': {
                            'page': page,
                            'total_pages': paginator.num_pages,
                            'total_count': paginator.count,
                            'has_next': page_obj.has_next(),
                            'has_previous': page_obj.has_previous()
                        }
                    }
                })
            
        except Exception as e:
            logger.error(f"Error in status API: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)

class ListPOsAPI(View):
    """API to list PO Numbers with attachments"""
    
    def get(self, request):
        """
        List PO Numbers that have attachments
        
        Query params:
        ?limit=20           // Max records to return
        ?unprocessed=true   // Only show unprocessed POs
        ?search=PO-2024     // Search PO numbers containing text
        """
        try:
            limit = min(int(request.GET.get('limit', 20)), 100)
            unprocessed_only = request.GET.get('unprocessed', 'false').lower() == 'true'
            search_term = request.GET.get('search', '')
            
            # Base query: POs with attachments
            queryset = ItemWiseGrn.objects.filter(
                Q(attachment_1__isnull=False) |
                Q(attachment_2__isnull=False) |
                Q(attachment_3__isnull=False) |
                Q(attachment_4__isnull=False) |
                Q(attachment_5__isnull=False)
            )
            
            # Search filter
            if search_term:
                queryset = queryset.filter(po_no__icontains=search_term)
            
            if unprocessed_only:
                # Exclude POs that already have invoice records
                processed_grn_ids = InvoiceData.objects.values_list('source_grn_id', flat=True).distinct()
                queryset = queryset.exclude(id__in=processed_grn_ids)
            
            # Group by PO number and get summary
            po_summaries = {}
            for grn in queryset.order_by('po_no'):
                po_no = grn.po_no
                
                if po_no not in po_summaries:
                    po_summaries[po_no] = {
                        'po_number': po_no,
                        'supplier': grn.supplier,
                        'grn_count': 0,
                        'attachment_count': 0,
                        'processed_count': 0,
                        'sample_grn_ids': [],
                        'fully_processed': True
                    }
                
                po_summaries[po_no]['grn_count'] += 1
                po_summaries[po_no]['sample_grn_ids'].append(grn.id)
                
                # Count attachments for this GRN
                grn_attachments = 0
                grn_processed = 0
                
                for i in range(1, 6):
                    if getattr(grn, f'attachment_{i}'):
                        grn_attachments += 1
                        po_summaries[po_no]['attachment_count'] += 1
                        
                        # Check if processed
                        if InvoiceData.objects.filter(source_grn_id=grn, attachment_number=str(i)).exists():
                            grn_processed += 1
                            po_summaries[po_no]['processed_count'] += 1
                
                # Update fully_processed flag
                if grn_attachments > grn_processed:
                    po_summaries[po_no]['fully_processed'] = False
            
            # Convert to list and limit results
            results = list(po_summaries.values())[:limit]
            
            return JsonResponse({
                'success': True,
                'data': {
                    'total_found': len(results),
                    'showing_unprocessed_only': unprocessed_only,
                    'search_term': search_term,
                    'pos': results
                }
            })
            
        except Exception as e:
            logger.error(f"Error in list POs API: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)


