# In your main project's urls.py (reconciliation/urls.py)

from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    
    # Updated to match your app name in INSTALLED_APPS
    path('document-processing/', include('document_processing.urls')),
    
    # OR if you want shorter URLs without the prefix:
    # path('', include('document_processing.urls')),
]