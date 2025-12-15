from pydantic import BaseModel, Field, field_validator
from datetime import datetime 
from typing import Optional
import pandas as pd

class RealEstateDataRaw(BaseModel):
    """Raw layer schema"""
    serial_number: Optional[int] = None
    list_year: Optional[int] = None
    date_recorded: Optional[str] = None
    town: Optional[str] = None
    address: Optional[str] = None
    assessed_value: Optional[str] = None
    sale_amount: Optional[str] = None
    sales_ratio: Optional[float] = None
    property_type: Optional[str] = None
    residential_type: Optional[str] = None
    non_use_code: Optional[str] = None
    assessor_remarks: Optional[str] = None
    opm_remarks: Optional[str] = None
    location: Optional[str] = None

class RealEstateDataClean(BaseModel):
    """Clean layer schema"""
    serial_number: int
    list_year: int
    date_recorded: str
    town: str
    address: str
    assessed_value: int 
    sale_amount: int
    sales_ratio: float
    property_type: str
    residential_type: Optional[str] = None
    non_use_code: Optional[str] = None
    assessor_remarks: Optional[str] = None
    opm_remarks: Optional[str] = None
    location: Optional[str] = None

    @field_validator('sale_amount')
    @classmethod
    def validate_sale_amount(cls, v: int) -> int:
        if v < 0:
            raise ValueError('sale_amount must be non-negative')
        return v
    
    @field_validator('assessed_value')
    @classmethod
    def validate_assessed_value(cls, v: int) -> int:
        if v < 0:
            raise ValueError('assessed_value must be non-negative')
        return v