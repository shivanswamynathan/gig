from django.urls import path
from . import views

urlpatterns = [
    path('process/', views.process_document, name='process_document'),
]