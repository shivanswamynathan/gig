import pandas as pd
import json
import logging
import tempfile
import os
from typing import Dict, Any, List, Union
from django.conf import settings
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

class POGRNExtractor:
    """
    Purchase Order and Goods Receipt Note data extractor for Excel/CSV files
    """
    
    def __init__(self):
        """Initialize the extractor"""
        # Define expected columns for PO and GRN data
        self.po_columns = {
            'po_number': ['po_number', 'po_no', 'po no.', 'purchase_order_number', 'order_number'],
            'po_date': ['po_date', 'po_creation_date', 'order_date', 'purchase_date', 'date'],
            'vendor_name': ['vendor_name', 'vendor', 'supplier_name', 'supplier'],
            'vendor_code': ['vendor_code', 'vendor_id', 'supplier_code', 'supplier_id'],
            'item_code': ['item_code', 'item_id', 'product_code', 'sku'],
            'item_description': ['item_description', 'description', 'product_name', 'item_name'],
            'quantity': ['quantity', 'qty', 'ordered_qty', 'order_quantity', 'no_item_in_po'],
            'unit': ['unit', 'uom', 'unit_of_measure'],
            'rate': ['rate', 'unit_price', 'price', 'cost'],
            'amount': ['amount', 'total', 'line_total', 'total_amount', 'po_amount'],
            'delivery_date': ['delivery_date', 'expected_date', 'due_date'],
            'status': ['status', 'po_status', 'order_status'],
            'location': ['location', 'site', 'branch', 'warehouse'],
            'concerned_person': ['concerned_person', 'contact_person', 'buyer']
        }
        
        self.grn_columns = {
            'grn_number': ['grn_number', 'grn_no', 'grn no.', 'receipt_number', 'goods_receipt_no'],
            'grn_date': ['grn_date', 'grn_creation_date', 'receipt_date', 'received_date', 'date'],
            'po_number': ['po_number', 'po_no', 'po no.', 'purchase_order_number', 'reference_po'],
            'vendor_name': ['vendor_name', 'vendor', 'supplier_name', 'supplier'],
            'item_code': ['item_code', 'item_id', 'product_code', 'sku'],
            'item_description': ['item_description', 'description', 'product_name', 'item_name'],
            'received_quantity': ['received_quantity', 'received_qty', 'qty_received', 'quantity', 'no_item_in_grn'],
            'accepted_quantity': ['accepted_quantity', 'accepted_qty', 'qty_accepted'],
            'rejected_quantity': ['rejected_quantity', 'rejected_qty', 'qty_rejected'],
            'unit': ['unit', 'uom', 'unit_of_measure'],
            'rate': ['rate', 'unit_price', 'price', 'cost'],
            'amount': ['amount', 'total', 'line_total', 'total_amount', 'grn_amount'],
            'subtotal': ['subtotal', 'grn_subtotal', 'net_amount'],
            'tax': ['tax', 'grn_tax', 'tax_amount', 'gst'],
            'batch_number': ['batch_number', 'batch_no', 'lot_number'],
            'expiry_date': ['expiry_date', 'exp_date', 'expiration_date'],
            'inspector': ['inspector', 'checked_by', 'quality_inspector'],
            'remarks': ['remarks', 'comments', 'notes'],
            'received_status': ['received_status', 'status', 'grn_status'],
            'location': ['location', 'site', 'branch', 'warehouse']
        }
        
        # Define combined PO-GRN columns for reports like yours
        self.combined_po_grn_columns = {
            's_no': ['s.no.', 's_no', 'serial_number', 'sr_no'],
            'location': ['location', 'site', 'branch', 'warehouse'],
            'po_number': ['po no.', 'po_number', 'po_no', 'purchase_order_number'],
            'po_creation_date': ['po_creation_date', 'po_date', 'order_date'],
            'no_item_in_po': ['no_item_in_po', 'po_items', 'po_line_count'],
            'po_amount': ['po_amount', 'po_total', 'order_amount'],
            'po_status': ['po_status', 'order_status', 'po_state'],
            'supplier_name': ['supplier_name', 'vendor_name', 'vendor'],
            'concerned_person': ['concerned person', 'concerned_person', 'contact_person'],
            'grn_no': ['grn_no', 'grn_number', 'receipt_number'],
            'grn_creation_date': ['grn_creation_date', 'grn_date', 'receipt_date'],
            'no_item_in_grn': ['no_item_in_grn', 'grn_items', 'grn_line_count'],
            'received_status': ['received status', 'received_status', 'grn_status'],
            'grn_subtotal': ['grn_subtotal', 'subtotal', 'net_amount'],
            'grn_tax': ['grn_tax', 'tax_amount', 'gst'],
            'grn_amount': ['grn_amount', 'grn_total', 'receipt_amount']
        }
    
    def detect_file_type(self, filename: str) -> str:
        """
        Detect file type based on extension
        
        Args:
            filename: Name of the file
            
        Returns:
            File type ('excel' or 'csv')
        """
        extension = filename.lower().split('.')[-1]
        if extension in ['xlsx', 'xls']:
            return 'excel'
        elif extension == 'csv':
            return 'csv'
        else:
            raise ValueError(f"Unsupported file format: {extension}. Supported formats: xlsx, xls, csv")
    
    def normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize column names by removing spaces, converting to lowercase
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with normalized column names
        """
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')
        return df
    
    def map_columns(self, df: pd.DataFrame, column_mapping: Dict[str, List[str]]) -> Dict[str, str]:
        """
        Map DataFrame columns to standard column names
        
        Args:
            df: Input DataFrame
            column_mapping: Dictionary mapping standard names to possible column variations
            
        Returns:
            Dictionary mapping found columns to standard names
        """
        column_map = {}
        df_columns = df.columns.tolist()
        
        for standard_name, possible_names in column_mapping.items():
            for possible_name in possible_names:
                if possible_name in df_columns:
                    column_map[possible_name] = standard_name
                    break
        
        return column_map
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the data by handling missing values and data types
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        # Replace NaN values with empty strings for string columns
        string_columns = df.select_dtypes(include=['object']).columns
        df[string_columns] = df[string_columns].fillna('')
        
        # Replace NaN values with 0 for numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
        
        # Convert date columns
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                df[col] = df[col].dt.strftime('%d/%m/%Y').fillna('')
        
        return df
    
    def detect_data_type(self, df: pd.DataFrame) -> str:
        """
        Detect whether the data is PO, GRN, or combined PO-GRN based on column names
        
        Args:
            df: Input DataFrame
            
        Returns:
            Data type ('po', 'grn', 'combined_po_grn', or 'unknown')
        """
        df_columns = set(df.columns.tolist())
        
        po_indicators = set(['po_number', 'po_no', 'purchase_order_number', 'order_number', 'po_creation_date'])
        grn_indicators = set(['grn_number', 'grn_no', 'receipt_number', 'goods_receipt_no', 'received_quantity', 'grn_creation_date'])
        combined_indicators = set(['po_amount', 'grn_amount', 'grn_subtotal', 'grn_tax', 'po_status', 'received_status'])
        
        po_matches = len(po_indicators.intersection(df_columns))
        grn_matches = len(grn_indicators.intersection(df_columns))
        combined_matches = len(combined_indicators.intersection(df_columns))
        
        # Check if it's a combined PO-GRN report (like your file)
        if combined_matches >= 3 and po_matches > 0 and grn_matches > 0:
            return 'combined_po_grn'
        elif grn_matches > po_matches:
            return 'grn'
        elif po_matches > 0:
            return 'po'
        else:
            return 'unknown'
    
    def process_po_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Process Purchase Order data
        
        Args:
            df: DataFrame containing PO data
            
        Returns:
            Structured PO data
        """
        # Map columns
        column_map = self.map_columns(df, self.po_columns)
        
        # Rename columns to standard names
        df_renamed = df.rename(columns=column_map)
        
        # Clean data
        df_cleaned = self.clean_data(df_renamed)
        
        # Convert to records
        po_records = df_cleaned.to_dict('records')
        
        # Calculate summary statistics
        summary = {
            'total_records': len(po_records),
            'unique_pos': df_cleaned['po_number'].nunique() if 'po_number' in df_cleaned.columns else 0,
            'unique_vendors': df_cleaned['vendor_name'].nunique() if 'vendor_name' in df_cleaned.columns else 0,
            'total_amount': df_cleaned['amount'].sum() if 'amount' in df_cleaned.columns else 0,
            'date_range': {
                'from': df_cleaned['po_date'].min() if 'po_date' in df_cleaned.columns else '',
                'to': df_cleaned['po_date'].max() if 'po_date' in df_cleaned.columns else ''
            }
        }
        
        return {
            'data_type': 'purchase_order',
            'records': po_records,
            'summary': summary,
            'columns_mapped': column_map,
            'total_records': len(po_records)
        }
    
    def process_combined_po_grn_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Process combined PO-GRN data (like your Excel file)
        
        Args:
            df: DataFrame containing combined PO-GRN data
            
        Returns:
            Structured combined PO-GRN data
        """
        # Map columns
        column_map = self.map_columns(df, self.combined_po_grn_columns)
        
        # Rename columns to standard names
        df_renamed = df.rename(columns=column_map)
        
        # Clean data
        df_cleaned = self.clean_data(df_renamed)
        
        # Convert to records
        records = df_cleaned.to_dict('records')
        
        # Calculate summary statistics
        summary = {
            'total_records': len(records),
            'unique_pos': df_cleaned['po_number'].nunique() if 'po_number' in df_cleaned.columns else 0,
            'unique_grns': df_cleaned['grn_no'].nunique() if 'grn_no' in df_cleaned.columns else 0,
            'unique_suppliers': df_cleaned['supplier_name'].nunique() if 'supplier_name' in df_cleaned.columns else 0,
            'unique_locations': df_cleaned['location'].nunique() if 'location' in df_cleaned.columns else 0,
            'total_po_amount': df_cleaned['po_amount'].sum() if 'po_amount' in df_cleaned.columns else 0,
            'total_grn_amount': df_cleaned['grn_amount'].sum() if 'grn_amount' in df_cleaned.columns else 0,
            'total_grn_subtotal': df_cleaned['grn_subtotal'].sum() if 'grn_subtotal' in df_cleaned.columns else 0,
            'total_grn_tax': df_cleaned['grn_tax'].sum() if 'grn_tax' in df_cleaned.columns else 0,
            'po_status_breakdown': df_cleaned['po_status'].value_counts().to_dict() if 'po_status' in df_cleaned.columns else {},
            'received_status_breakdown': df_cleaned['received_status'].value_counts().to_dict() if 'received_status' in df_cleaned.columns else {},
            'date_range': {
                'po_from': df_cleaned['po_creation_date'].min() if 'po_creation_date' in df_cleaned.columns else '',
                'po_to': df_cleaned['po_creation_date'].max() if 'po_creation_date' in df_cleaned.columns else '',
                'grn_from': df_cleaned['grn_creation_date'].min() if 'grn_creation_date' in df_cleaned.columns else '',
                'grn_to': df_cleaned['grn_creation_date'].max() if 'grn_creation_date' in df_cleaned.columns else ''
            }
        }
        
        return {
            'data_type': 'combined_po_grn',
            'records': records,
            'summary': summary,
            'columns_mapped': column_map,
            'total_records': len(records)
        }
    
    def process_grn_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Process Goods Receipt Note data
        
        Args:
            df: DataFrame containing GRN data
            
        Returns:
            Structured GRN data
        """
        # Map columns
        column_map = self.map_columns(df, self.grn_columns)
        
        # Rename columns to standard names
        df_renamed = df.rename(columns=column_map)
        
        # Clean data
        df_cleaned = self.clean_data(df_renamed)
        
        # Convert to records
        grn_records = df_cleaned.to_dict('records')
        
        # Calculate summary statistics
        summary = {
            'total_records': len(grn_records),
            'unique_grns': df_cleaned['grn_number'].nunique() if 'grn_number' in df_cleaned.columns else 0,
            'unique_pos': df_cleaned['po_number'].nunique() if 'po_number' in df_cleaned.columns else 0,
            'unique_vendors': df_cleaned['vendor_name'].nunique() if 'vendor_name' in df_cleaned.columns else 0,
            'total_received_qty': df_cleaned['received_quantity'].sum() if 'received_quantity' in df_cleaned.columns else 0,
            'total_accepted_qty': df_cleaned['accepted_quantity'].sum() if 'accepted_quantity' in df_cleaned.columns else 0,
            'total_rejected_qty': df_cleaned['rejected_quantity'].sum() if 'rejected_quantity' in df_cleaned.columns else 0,
            'date_range': {
                'from': df_cleaned['grn_date'].min() if 'grn_date' in df_cleaned.columns else '',
                'to': df_cleaned['grn_date'].max() if 'grn_date' in df_cleaned.columns else ''
            }
        }
        
        return {
            'data_type': 'goods_receipt_note',
            'records': grn_records,
            'summary': summary,
            'columns_mapped': column_map,
            'total_records': len(grn_records)
        }
    
    def read_file(self, file_path: str, file_type: str) -> pd.DataFrame:
        """
        Read file and return DataFrame, handling special formats like your Excel file
        
        Args:
            file_path: Path to the file
            file_type: Type of file ('excel' or 'csv')
            
        Returns:
            DataFrame containing the data
        """
        try:
            if file_type == 'excel':
                # First, read the file to detect the header row
                df_preview = pd.read_excel(file_path, header=None, engine='openpyxl')
                
                # Find the actual header row by looking for specific keywords
                header_row = None
                for i in range(min(15, len(df_preview))):  # Check first 15 rows
                    row_values = df_preview.iloc[i].dropna().astype(str).tolist()
                    row_text = ' '.join(row_values).lower()
                    
                    # Look for header indicators specific to your file format
                    header_indicators = ['s.no.', 'location', 'po no.', 'po_creation_date', 
                                       'supplier_name', 'grn_no', 'grn_creation_date']
                    
                    if any(indicator in row_text for indicator in header_indicators):
                        header_row = i
                        break
                
                if header_row is not None:
                    # Read the file again with the correct header row
                    df = pd.read_excel(file_path, header=header_row, engine='openpyxl')
                    logger.info(f"Found headers at row {header_row + 1}")
                else:
                    # If no specific header found, try the default
                    df = pd.read_excel(file_path, engine='openpyxl')
                    logger.warning("Could not detect header row, using default")
                
            elif file_type == 'csv':
                # Try different encodings for CSV
                encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
                df = None
                
                for encoding in encodings:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                
                if df is None:
                    raise ValueError("Could not read CSV file with any supported encoding")
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            # Remove rows where all values are NaN or empty
            df = df[df.iloc[:, 0].notna()]  # Keep rows where first column is not NaN
            
            # Normalize column names
            df = self.normalize_column_names(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise Exception(f"Failed to read file: {str(e)}")
    
    def process_uploaded_file(self, uploaded_file) -> Dict[str, Any]:
        """
        Process Django uploaded file
        
        Args:
            uploaded_file: Django UploadedFile object
            
        Returns:
            Dictionary containing extracted data
        """
        temp_path = None
        try:
            # Detect file type
            file_type = self.detect_file_type(uploaded_file.name)
            
            # Save uploaded file to temporary location
            suffix = '.xlsx' if file_type == 'excel' else '.csv'
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
                for chunk in uploaded_file.chunks():
                    temp_file.write(chunk)
                temp_path = temp_file.name
            
            # Read the file
            logger.info(f"Reading {file_type} file: {uploaded_file.name}")
            df = self.read_file(temp_path, file_type)
            
            if df.empty:
                raise ValueError("The uploaded file is empty or contains no data")
            
            # Detect data type (PO, GRN, or combined)
            data_type = self.detect_data_type(df)
            
            if data_type == 'unknown':
                logger.warning(f"Could not determine data type for file: {uploaded_file.name}")
                # Process as generic data
                df_cleaned = self.clean_data(df)
                records = df_cleaned.to_dict('records')
                
                result = {
                    'data_type': 'unknown',
                    'records': records,
                    'summary': {
                        'total_records': len(records),
                        'columns': df.columns.tolist()
                    },
                    'total_records': len(records)
                }
            elif data_type == 'po':
                logger.info("Processing as Purchase Order data")
                result = self.process_po_data(df)
            elif data_type == 'grn':
                logger.info("Processing as Goods Receipt Note data")
                result = self.process_grn_data(df)
            else:  # combined_po_grn
                logger.info("Processing as Combined PO-GRN data")
                result = self.process_combined_po_grn_data(df)
            
            # Add processing metadata
            result["_metadata"] = {
                "filename": uploaded_file.name,
                "file_size": uploaded_file.size,
                "file_type": file_type,
                "processed_at": datetime.now().isoformat(),
                "processing_status": "success",
                "detected_data_type": data_type,
                "original_columns": df.columns.tolist(),
                "rows_processed": len(df)
            }
            
            logger.info(f"Successfully processed file: {uploaded_file.name}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing uploaded file {uploaded_file.name}: {str(e)}")
            
            # Return error information
            error_data = {
                "data_type": "error",
                "records": [],
                "summary": {},
                "total_records": 0,
                "_metadata": {
                    "filename": uploaded_file.name,
                    "file_size": getattr(uploaded_file, 'size', 0),
                    "processed_at": datetime.now().isoformat(),
                    "processing_status": "failed",
                    "error_message": str(e)
                }
            }
            
            raise Exception(f"Failed to process file: {str(e)}")
            
        finally:
            # Clean up temporary file
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception as cleanup_error:
                    logger.warning(f"Failed to cleanup temp file: {cleanup_error}")
    
    def process_file_path(self, file_path: str) -> Dict[str, Any]:
        """
        Process file from file path (for testing purposes)
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary containing extracted data
        """
        try:
            # Detect file type
            file_type = self.detect_file_type(file_path)
            
            # Read the file
            logger.info(f"Reading {file_type} file: {file_path}")
            df = self.read_file(file_path, file_type)
            
            if df.empty:
                raise ValueError("The file is empty or contains no data")
            
            # Detect data type (PO, GRN, or combined)
            data_type = self.detect_data_type(df)
            
            if data_type == 'po':
                result = self.process_po_data(df)
            elif data_type == 'grn':
                result = self.process_grn_data(df)
            elif data_type == 'combined_po_grn':
                result = self.process_combined_po_grn_data(df)
            else:
                # Process as generic data
                df_cleaned = self.clean_data(df)
                records = df_cleaned.to_dict('records')
                
                result = {
                    'data_type': 'unknown',
                    'records': records,
                    'summary': {
                        'total_records': len(records),
                        'columns': df.columns.tolist()
                    },
                    'total_records': len(records)
                }
            
            # Add processing metadata
            result["_metadata"] = {
                "file_path": file_path,
                "file_type": file_type,
                "processed_at": datetime.now().isoformat(),
                "processing_status": "success",
                "detected_data_type": data_type,
                "original_columns": df.columns.tolist(),
                "rows_processed": len(df)
            }
            
            logger.info(f"Successfully processed file: {file_path}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            raise Exception(f"Failed to process file: {str(e)}")