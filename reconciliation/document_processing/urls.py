from django.urls import path
from . import views

app_name = 'document_processing'

urlpatterns = [
    # Main processing endpoints
    path('api/process-invoice/', views.ProcessInvoiceAPI.as_view(), name='process_invoice'),
    path('api/process-po-grn/', views.ProcessPOGRNAPI.as_view(), name='process_po_grn'),
    path('api/bulk-process/', views.BulkProcessAPI.as_view(), name='bulk_process'),
]