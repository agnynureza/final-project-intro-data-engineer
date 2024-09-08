import requests
import luigi
import pandas as pd
import numpy as np
import re
import ast
import logging
import time
from sqlalchemy import create_engine
from pangres import upsert
from datetime import datetime
from sqlalchemy.exc import DataError
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def db_source_postgres_engine():
    engine = create_engine("postgresql://postgres:password123@localhost:5499/etl_db")

    return engine

def data_warehouse_postgres_engine():
    engine = create_engine("postgresql://postgres:12345@localhost:5432/pacmaan", echo=False)

    return engine

class ExtractDBAmazonSalesData(luigi.Task):
    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget("data/raw/extract_amazon_sales_data.csv")
    
    def run(self):

        engine = db_source_postgres_engine()
        
        create_engine("postgresql://postgres:password123@localhost:5499/etl_db")

        query_sales= "SELECT * FROM amazon_sales_data"

        extract_sales_data = pd.read_sql(sql = query_sales,
                                               con = engine)

        extract_sales_data.to_csv(self.output().path, index = False)

class ExtractAPIUniversitiesData(luigi.Task):
    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget("data/raw/extract_universities_data.csv")

    def run(self):

        resp = requests.get("http://universities.hipolabs.com/search?country=Indonesia")

        raw_uni_data = resp.json()

        extract_uni_data = pd.DataFrame(raw_uni_data)

        extract_uni_data.to_csv(self.output().path, index = False)
        
class ExtractCsvFileElectronicProducts(luigi.Task):
    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget("data/raw/electronics_products_data.csv")

    def run(self):
        pass


def validatation_process(data, table_name):
    print("========== Start Pipeline Validation ==========")
    print("")

    n_rows = data.shape[0]
    n_cols = data.shape[1]

    print(f"Pada table {table_name} memiliki jumlah {n_rows} baris dan {n_cols} kolom")
    print("")

    GET_COLS = data.columns

    for col in GET_COLS:
        print(f"Column {col} has data type {data[col].dtypes}")

    print("")

    for col in GET_COLS:
        get_missing_values = (data[col].isnull().sum() * 100) / len(data)
        print(f"Columns {col} has percentages missing values {get_missing_values} %")

    print("")
    print("========== End Pipeline Validation ==========")
    
class ValidateData(luigi.Task):

    def requires(self):
        return [ExtractDBAmazonSalesData(),
                ExtractAPIUniversitiesData(),
                ExtractCsvFileElectronicProducts()]

    def output(self):
        pass

    def run(self):
        validate_amazon_data = pd.read_csv(self.input()[0].path)

        validate_universities_data = pd.read_csv(self.input()[1].path)

        validate_electronic_data = pd.read_csv(self.input()[2].path)

        validatation_process(data = validate_amazon_data,
                             table_name = "amazon_sales")

        validatation_process(data = validate_universities_data,
                             table_name = "universities")

        validatation_process(data = validate_electronic_data,
                             table_name = "electronic_products")


class TransformAmazonSalesData(luigi.Task):

    def requires(self):
        return ExtractDBAmazonSalesData()

    def output(self):
        return luigi.LocalTarget("data/transform/transform_amazon_sales_data.csv")

    def run(self):
        amazon_sales_data = pd.read_csv(self.input().path)

        rename_cols = {
            "name": "product_name",
            "no_of_ratings": "number_of_ratings",
        }

        amazon_sales_data = amazon_sales_data.rename(columns=rename_cols)
        
        selected_columns = ["main_category", "sub_category", "ratings",
                            "number_of_ratings", "discount_price", "actual_price"]

        amazon_sales_data = amazon_sales_data[selected_columns]

        amazon_sales_data["discount_price"] = amazon_sales_data["discount_price"].str.replace("₹", "")
        amazon_sales_data["actual_price"] = amazon_sales_data["actual_price"].str.replace("₹", "")
        
        validate_value = ['ratings', 'number_of_ratings', 'discount_price', 'actual_price']
        for column in validate_value:
            amazon_sales_data[column] = pd.to_numeric(amazon_sales_data[column], errors='coerce')
        
        casting_cols = {
            "number_of_ratings": "float64",
            "ratings": "float64",
            "discount_price": "float64",
            "actual_price": "float64"
        }

        amazon_sales_data = amazon_sales_data.astype(casting_cols)

        columns_missing = ['ratings', 'number_of_ratings', 'discount_price', 'actual_price']
        for column in columns_missing:
            median_value = amazon_sales_data[column].median()
            amazon_sales_data[column] = amazon_sales_data[column].fillna(median_value)

        validatation_process(data=amazon_sales_data,
                             table_name="amazon_sales")

        amazon_sales_data.insert(0, 'id', range(1, len(amazon_sales_data)+1))
        amazon_sales_data.to_csv(self.output().path, index=False)

class TransformUniversityData(luigi.Task):

    def requires(self):
        return ExtractAPIUniversitiesData()

    def output(self):
        return luigi.LocalTarget("data/transform/transform_universities_data.csv")

    def run(self):
        university_data = pd.read_csv(self.input().path)

        rename_cols = {
            "domains": "domain",
            "web_pages": "website",
        }

        university_data = university_data.rename(columns=rename_cols)
        
        selected_columns = ["name", "domain", "website","country"]

        university_data = university_data[selected_columns]

        university_data["domain"] = university_data["domain"].str.replace(r'[\[\]\']', '', regex=True)
        university_data["website"] = university_data["website"].str.replace(r'[\[\]\']', '', regex=True)
        

        validatation_process(data=university_data,
                             table_name="universities")

        university_data.insert(0, 'id', range(1, len(university_data)+1))
        university_data.to_csv(self.output().path, index=False)
        
        
def clean_weight(weight_str):
    if pd.isna(weight_str):
        return np.nan
    
    weight_str = weight_str.lower().strip()
    
    match = re.match(r'([\d.]+)\s*([a-z]+)', weight_str)
    if not match:
        return np.nan
    
    value, unit = match.groups()
    value = float(value)
    
    if 'oz' in unit or 'ounces' in unit:
        return value / 16
    elif 'lb' in unit or 'pounds' in unit:
        return value
    elif 'g' in unit:
        return value / 453.592 # 1 pound = 453.592 grams
    else:
        return np.nan
    

class TransformElectronicsData(luigi.Task):

    def requires(self):
        return ExtractCsvFileElectronicProducts()

    def output(self):
        return luigi.LocalTarget("data/transform/transform_electronics_products_data.csv")

    def run(self):
        electronic_data = pd.read_csv(self.input().path)

        rename_cols = {
            "id": "electronic_id",
            "name": "product_name",
            "weight": "weight_gram",
            "categories": "category_name",
            "manufacturer": "manufacturer",
            "prices.currency": "currency",
            "prices.amountMax": "price",
            "prices.availability": "availability",
            "prices.condition": "condition",
            "prices.dateSeen": "release_date",
            "prices.isSale": "is_sale",
        }

        electronic_data = electronic_data.rename(columns=rename_cols)
        
        selected_columns = ["electronic_id", "upc", "weight_gram","product_name", "category_name", "manufacturer",
                            "currency", "price", "availability", "condition", "release_date", "is_sale"]

        electronic_data = electronic_data[selected_columns]
        
        electronic_data["weight_gram"] = electronic_data["weight_gram"].apply(clean_weight)
        
        electronic_data["upc"] = electronic_data["upc"].str.replace(',', '')
            
        electronic_data["upc"] = pd.to_numeric(electronic_data["upc"], errors='coerce')
        
        electronic_data['category_name'] = electronic_data['category_name'].apply(lambda x: np.array(x.split(',')) if isinstance(x, str) else np.array([]))
        
        electronic_data['release_date'] = electronic_data['release_date'].apply(lambda x: pd.to_datetime(x.split(',')[0], format='%Y-%m-%dT%H:%M:%SZ'))
        
        casting_cols = {
            "upc": "float64",
            "weight_gram": "float64",
            "price": "float64",
            "release_date": "datetime64[ns]",
            "is_sale": "boolean",
            "category_name": "object" 
        }

        electronic_data = electronic_data.astype(casting_cols)

        columns_missing = ['upc', 'weight_gram']
        for column in columns_missing:
            electronic_data[column] = electronic_data[column].fillna(value = 0)

        electronic_data["manufacturer"] = electronic_data["manufacturer"].fillna(value="unknown")
        validatation_process(data=electronic_data,
                             table_name="electronic_products")

        electronic_data.insert(0, 'id', range(1, len(electronic_data)+1))
        
        electronic_data.to_csv(self.output().path, index=False)

luigi.build([TransformElectronicsData()], local_scheduler = True)

def format_category(category_str):
    try:
        category_list = ast.literal_eval(category_str)
        return '{' + ','.join(f'"{c.strip()}"' for c in category_list if c.strip()) + '}'
    except:
        return '{}'
    
class LoadElectornicData(luigi.Task):

    def requires(self):
        return TransformElectronicsData()

    def output(self):
        return luigi.LocalTarget("data/load/load_electronics_products_data.csv")

    def run(self):
        load_electronic_data = pd.read_csv(self.input().path)
        load_electronic_data['created_at'] = datetime.now()
        
        load_electronic_data['category_name'] = load_electronic_data['category_name'].apply(format_category)

        load_electronic_data = load_electronic_data.set_index("id")

        dw_engine = data_warehouse_postgres_engine()

        dw_table_name = "dw_electronic_products"
        
        upsert(con=dw_engine,
                df=load_electronic_data,
                table_name=dw_table_name,
                if_row_exists="update",
                chunksize=2000)
            
        load_electronic_data.to_csv(self.output().path, index = False)

class LoadAmazonSalesData(luigi.Task):

    def requires(self):
        return TransformAmazonSalesData()

    def output(self):
        return luigi.LocalTarget("data/load/load_amazon_sales_data.csv")

    def run(self):
        load_amazon_data = pd.read_csv(self.input().path)
        load_amazon_data['created_at'] = datetime.now()

        load_amazon_data = load_amazon_data.set_index("id")

        dw_engine = data_warehouse_postgres_engine()

        dw_table_name = "dw_amazon_sales"
        
        upsert(con=dw_engine,
                df=load_amazon_data,
                table_name=dw_table_name,
                if_row_exists="update",
                chunksize=2000)
            
        load_amazon_data.to_csv(self.output().path, index = False)
        
class LoadUniversityData(luigi.Task):

    def requires(self):
        return TransformUniversityData()

    def output(self):
        return luigi.LocalTarget("data/load/load_universities_data.csv")

    def run(self):
        load_university_data = pd.read_csv(self.input().path)
        load_university_data['created_at'] = datetime.now()

        load_university_data = load_university_data.set_index("id")

        dw_engine = data_warehouse_postgres_engine()

        dw_table_name = "dw_universities"
        
        upsert(con=dw_engine,
                df=load_university_data,
                table_name=dw_table_name,
                if_row_exists="update",
                chunksize=2000)
            
        load_university_data.to_csv(self.output().path, index = False)

luigi.build([
    ExtractDBAmazonSalesData(),
    ExtractAPIUniversitiesData(),
    ExtractCsvFileElectronicProducts(),
    ValidateData(),
    TransformAmazonSalesData(),
    TransformUniversityData(),
    TransformElectronicsData(),
    LoadUniversityData(),
    LoadAmazonSalesData(),
    LoadElectornicData()], local_scheduler = True)
