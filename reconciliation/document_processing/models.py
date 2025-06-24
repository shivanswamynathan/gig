from django.db import models
from django.core.validators import MinValueValidator
from decimal import Decimal


class PoGrn(models.Model):
    """
    Model to store PO-GRN data from Excel/CSV uploads
    """
    
    # PO Information
    s_no = models.IntegerField(
        verbose_name="Serial Number",
        validators=[MinValueValidator(1)],
        help_text="Serial number from the uploaded file"
    )
    
    location = models.CharField(
        max_length=255,
        verbose_name="Location",
        help_text="Store/warehouse location"
    )
    
    po_number = models.CharField(
        max_length=100,
        verbose_name="PO Number",
        db_index=True,
        help_text="Purchase Order Number"
    )
    
    po_creation_date = models.DateField(
        verbose_name="PO Creation Date",
        help_text="Date when the PO was created"
    )
    
    no_item_in_po = models.IntegerField(
        verbose_name="Number of Items in PO",
        validators=[MinValueValidator(0)],
        help_text="Total number of items in the purchase order"
    )
    
    po_amount = models.DecimalField(
        max_digits=15,
        decimal_places=2,
        verbose_name="PO Amount",
        validators=[MinValueValidator(Decimal('0.00'))],
        help_text="Total amount of the purchase order"
    )
    
    po_status = models.CharField(
        max_length=50,
        verbose_name="PO Status",
        help_text="Status of the purchase order (e.g., Completed, In Process)"
    )
    
    supplier_name = models.CharField(
        max_length=255,
        verbose_name="Supplier Name",
        db_index=True,
        help_text="Name of the supplier/vendor"
    )
    
    concerned_person = models.CharField(
        max_length=255,
        verbose_name="Concerned Person",
        blank=True,
        null=True,
        help_text="Person responsible for the PO"
    )
    
    # GRN Information
    grn_number = models.CharField(
        max_length=100,
        verbose_name="GRN Number",
        db_index=True,
        blank=True,
        null=True,
        help_text="Goods Receipt Note Number"
    )
    
    grn_creation_date = models.DateField(
        verbose_name="GRN Creation Date",
        blank=True,
        null=True,
        help_text="Date when the GRN was created"
    )
    
    no_item_in_grn = models.IntegerField(
        verbose_name="Number of Items in GRN",
        validators=[MinValueValidator(0)],
        blank=True,
        null=True,
        help_text="Total number of items in the goods receipt note"
    )
    
    received_status = models.CharField(
        max_length=50,
        verbose_name="Received Status",
        blank=True,
        null=True,
        help_text="Status of goods receipt (e.g., Received, Pending)"
    )
    
    grn_subtotal = models.DecimalField(
        max_digits=15,
        decimal_places=2,
        verbose_name="GRN Subtotal",
        validators=[MinValueValidator(Decimal('0.00'))],
        blank=True,
        null=True,
        help_text="Subtotal amount before tax in GRN"
    )
    
    grn_tax = models.DecimalField(
        max_digits=15,
        decimal_places=2,
        verbose_name="GRN Tax",
        validators=[MinValueValidator(Decimal('0.00'))],
        blank=True,
        null=True,
        help_text="Tax amount in GRN"
    )
    
    grn_amount = models.DecimalField(
        max_digits=15,
        decimal_places=2,
        verbose_name="GRN Amount",
        validators=[MinValueValidator(Decimal('0.00'))],
        blank=True,
        null=True,
        help_text="Total amount including tax in GRN"
    )
    
    # Upload metadata
    upload_batch_id = models.CharField(
        max_length=100,
        verbose_name="Upload Batch ID",
        db_index=True,
        help_text="Unique identifier for the upload session"
    )
    
    uploaded_filename = models.CharField(
        max_length=255,
        verbose_name="Uploaded Filename",
        help_text="Original filename of the uploaded file"
    )
    
    # Timestamps
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Created At"
    )
    
    updated_at = models.DateTimeField(
        auto_now=True,
        verbose_name="Updated At"
    )

    class Meta:
        db_table = 'po_grn'
        verbose_name = "PO GRN Record"
        verbose_name_plural = "PO GRN Records"
        ordering = ['s_no', 'po_creation_date']
        indexes = [
            models.Index(fields=['po_number']),
            models.Index(fields=['grn_number']),
            models.Index(fields=['supplier_name']),
            models.Index(fields=['upload_batch_id']),
            models.Index(fields=['po_creation_date']),
            models.Index(fields=['grn_creation_date']),
        ]
        
        # Unique constraint to prevent duplicate entries
        unique_together = [
            ['po_number', 'grn_number', 'upload_batch_id']
        ]

    def __str__(self):
        return f"PO: {self.po_number} - GRN: {self.grn_number or 'N/A'}"

    @property
    def po_grn_variance(self):
        """Calculate variance between PO amount and GRN amount"""
        if self.grn_amount:
            return self.po_amount - self.grn_amount
        return None

    @property
    def item_variance(self):
        """Calculate variance between PO items and GRN items"""
        if self.no_item_in_grn:
            return self.no_item_in_po - self.no_item_in_grn
        return None

    @property
    def is_fully_received(self):
        """Check if all items from PO are received in GRN"""
        return (
            self.received_status and 
            self.received_status.lower() == 'received' and
            self.no_item_in_grn == self.no_item_in_po
        )


class UploadHistory(models.Model):
    """
    Model to track file upload history
    """
    
    batch_id = models.CharField(
        max_length=100,
        unique=True,
        verbose_name="Batch ID",
        db_index=True
    )
    
    filename = models.CharField(
        max_length=255,
        verbose_name="Filename"
    )
    
    file_size = models.BigIntegerField(
        verbose_name="File Size (bytes)"
    )
    
    total_records = models.IntegerField(
        verbose_name="Total Records Processed",
        validators=[MinValueValidator(0)]
    )
    
    successful_records = models.IntegerField(
        verbose_name="Successful Records",
        validators=[MinValueValidator(0)]
    )
    
    failed_records = models.IntegerField(
        verbose_name="Failed Records",
        validators=[MinValueValidator(0)]
    )
    
    processing_status = models.CharField(
        max_length=20,
        choices=[
            ('processing', 'Processing'),
            ('completed', 'Completed'),
            ('failed', 'Failed'),
            ('partial', 'Partially Completed'),
        ],
        default='processing',
        verbose_name="Processing Status"
    )
    
    error_details = models.TextField(
        blank=True,
        null=True,
        verbose_name="Error Details"
    )
    
    uploaded_by = models.CharField(
        max_length=100,
        blank=True,
        null=True,
        verbose_name="Uploaded By"
    )
    
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Created At"
    )
    
    completed_at = models.DateTimeField(
        blank=True,
        null=True,
        verbose_name="Completed At"
    )

    class Meta:
        db_table = 'upload_history'
        verbose_name = "Upload History"
        verbose_name_plural = "Upload Histories"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.filename} - {self.processing_status}"

    @property
    def success_rate(self):
        """Calculate success rate of upload"""
        if self.total_records > 0:
            return (self.successful_records / self.total_records) * 100
        return 0