from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import declarative_base
from datetime import datetime


Base = declarative_base()

class University(Base):
    __tablename__ = 'universities'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    country = Column(String(100), nullable=False)
    website = Column(String(255), nullable=False)
    domain = Column(String(100), nullable=False)
    created_date = Column(DateTime, default=datetime.now)

class AmazonSale(Base):
    __tablename__ = 'amazon_sales'

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_name = Column(String(255), nullable=False)
    main_category = Column(String(100), nullable=False)
    sub_category = Column(String(100), nullable=True)
    discount_price = Column(Float, nullable=False)
    actual_size = Column(Float, nullable=False)
    quantity = Column(Integer, nullable=False)
    created_date = Column(DateTime, default=datetime.now)

class ElectronicProduct(Base):
    __tablename__ = 'electronic_products'

    id = Column(Integer, primary_key=True)
    product_name = Column(String(255), nullable=False)
    category_name = Column(String(255), nullable=False)
    upc = Column(Float, nullable=False)
    weight = Column(Float, nullable=False)
    manufacturer = Column(String(100), nullable=False)
    currency = Column(String(100), nullable=False)
    price = Column(Float, nullable=False)
    availability = Column(Integer, nullable=False)
    condition =  Column(Integer, nullable=False)
    release_date = Column(DateTime, nullable=False)
    is_sale = Column(Boolean, nullable=False)
    created_date = Column(DateTime, default=datetime.now)

engine = create_engine('postgresql://postgres:12345@localhost:5432/pacmaan')

Base.metadata.create_all(engine)