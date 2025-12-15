import pytest
from datetime import datetime
from pydantic import ValidationError
from src.schemas import RealEstateDataClean

def test_realEstate_sale() -> None:
    sale = RealEstateDataClean(
        serial_number=123,
        list_year=2023,
        date_recorded="2023-05-15",
        town="Glassboro",
        address="123 Apple St",
        assessed_value=200000,
        sale_amount=250000,
        sales_ratio=0.8,
        property_type="Residential",
    )
    assert sale.sale_amount == 250000
    assert sale.assessed_value == 200000

def test_invalid_sale_amount() -> None:
    with pytest.raises(ValidationError):
        RealEstateDataClean(
            serial_number=123,
            list_year=2023,
            date_recorded="2023-05-15",
            town="Glassboro",
            address="123 Main St",
            assessed_value=200000,
            sale_amount=-1000,  # Invalid
            sales_ratio=0.8,
            property_type="Residential"
        )

def test_invalid_assessed_value() -> None:
    with pytest.raises(ValidationError):
        RealEstateDataClean(
            serial_number=123,
            list_year=2023,
            date_recorded="2023-05-15",
            town="Glassboro",
            address="123 Main St",
            assessed_value=-50000,  # Invalid
            sale_amount=250000,
            sales_ratio=0.8,
            property_type="Residential"
        )
        