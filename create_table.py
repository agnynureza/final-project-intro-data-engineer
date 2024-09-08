from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, ARRAY, Text
from sqlalchemy.orm import declarative_base
from datetime import datetime, UTC
from sqlalchemy.schema import Index


Base = declarative_base()

class University(Base):
    __tablename__ = 'dw_universities'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    country = Column(String(100), nullable=False)
    website = Column(String(255), nullable=False)
    domain = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.now(UTC))
    
    __table_args__ = (Index('ix_dw_university_id', 'id'),)

class AmazonSale(Base):
    __tablename__ = 'dw_amazon_sales'

    id = Column(Integer, primary_key=True)
    main_category = Column(String(255), nullable=False)
    sub_category = Column(String(255), nullable=True)
    discount_price = Column(Float, nullable=False)
    actual_price = Column(Float, nullable=False)
    ratings = Column(Float, nullable=False)
    number_of_ratings = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.now(UTC))

    __table_args__ = (Index('ix_dw_amazon_sales_id', 'id'),)


class ElectronicProduct(Base):
    __tablename__ = 'dw_electronic_products'

    id = Column(Integer, primary_key=True)
    electronic_id = Column(String, nullable=False)
    product_name = Column(Text, nullable=False)
    category_name = Column(ARRAY(String), nullable=False)
    upc = Column(Float, nullable=True)
    weight_gram = Column(Float, nullable=True)
    manufacturer = Column(String(100), nullable=True)
    currency = Column(String(50), nullable=False)
    price = Column(Float, nullable=False)
    availability = Column(Text, nullable=False)
    condition = Column(Text, nullable=False) 
    release_date = Column(DateTime, nullable=False)
    is_sale = Column(Boolean, nullable=False)
    created_at = Column(DateTime, default=datetime.now(UTC))
    
    __table_args__ = (Index('ix_dw_electronic_product_id', 'id'),)

engine = create_engine('postgresql://postgres:12345@localhost:5432/pacmaan')

Base.metadata.create_all(engine)