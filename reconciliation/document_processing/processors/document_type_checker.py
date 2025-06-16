import re
from typing import Dict, Any, Literal, Optional


class DocumentTypeChecker:
    """
    Determines whether a document is a Purchase Order (PO) or an Invoice.
    """
    
    # Keywords that suggest a document is a Purchase Order
    PO_KEYWORDS = [
        'purchase order', 'p.o.', 'p.o', 'p/o',
        'order number', 'order no', 'order #',
        'ordered by', 'ship to', 'delivery date'
    ]
    
    # Keywords that suggest a document is an Invoice
    INVOICE_KEYWORDS = [
        'invoice', 'bill', 'tax invoice',
        'invoice number', 'invoice no', 'invoice #',
        'invoice date', 'payment due', 'amount due',
        'balance due', 'total due', 'payment terms'
    ]
    
    @staticmethod
    def determine_document_type(text: str) -> Literal['po', 'invoice', 'unknown']:
        """
        Analyzes document text to determine if it's a PO or an Invoice.
        
        Args:
            text: The extracted text from the document
            
        Returns:
            'po', 'invoice', or 'unknown' based on text analysis
        """
        # Convert to lowercase for case-insensitive matching
        text_lower = text.lower()
        
        # Count occurrences of keywords
        po_score = 0
        invoice_score = 0
        
        # Check for PO keywords
        for keyword in DocumentTypeChecker.PO_KEYWORDS:
            if keyword in text_lower:
                po_score += 1
                
        # Check for Invoice keywords
        for keyword in DocumentTypeChecker.INVOICE_KEYWORDS:
            if keyword in text_lower:
                invoice_score += 1
                
        # Determine document type based on keyword scores
        if po_score > invoice_score:
            return 'po'
        elif invoice_score > po_score:
            return 'invoice'
        else:
            # If tied or no keywords found, check for specific patterns
            
            # Check for invoice number pattern
            invoice_patterns = [
                r'\binvoice\s*(?:no|number|#|num)[\s:.]*\d+',
                r'\binv\s*(?:no|number|#|num)[\s:.]*\d+'
            ]
            
            for pattern in invoice_patterns:
                if re.search(pattern, text_lower):
                    return 'invoice'
            
            # Check for PO number pattern
            po_patterns = [
                r'\b(?:p(?:urchase)?[/\s.]?o(?:rder)?)[.\s:]*(?:no|number|#|num)[\s:.]*\d+',
                r'\border\s*(?:no|number|#|num)[\s:.]*\d+'
            ]
            
            for pattern in po_patterns:
                if re.search(pattern, text_lower):
                    return 'po'
                    
            # If still unclear, return unknown
            return 'unknown'