
# reconciliation/document_processing/urls.py

from django.urls import path
from . import views

app_name = 'document_processing'

urlpatterns = [
    # Main processing endpoints
    path('api/process-invoice/', views.ProcessInvoiceAPI.as_view(), name='process_invoice_api'),
    path('api/process-invoice-simple/', views.process_invoice_simple, name='process_invoice_simple'),
    
    # Utility endpoints
    path('api/health/', views.health_check, name='health_check'),
    path('api/info/', views.api_info, name='api_info'),
]


# If you need to include this in your main urls.py, add:
# from django.urls import path, include
# 
# urlpatterns = [
#     # ... your other URL patterns
#     path('document-processing/', include('reconciliation.document_processing.urls')),
# ]