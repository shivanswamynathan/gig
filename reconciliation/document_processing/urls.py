from django.urls import path
from .views import views,po_grn_views,itemwise_grn_views

app_name = 'document_processing'

urlpatterns = [
    path('api/process-invoice/', views.ProcessInvoiceAPI.as_view(), name='process_invoice'),

    # PO-GRN data processing
    path('api/process-po-grn/', po_grn_views.ProcessPoGrnAPI.as_view(), name='process_po_grn'),

    # Item-wise GRN data processing
    path('api/process-itemwise-grn/', itemwise_grn_views.ProcessItemWiseGrnAPI.as_view(), name='process_itemwise_grn'),

    
]
