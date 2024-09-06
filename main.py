import requests
import luigi
import pandas as pd
from sqlalchemy import create_engine

def db_source_postgres_engine():
    engine = create_engine("postgresql://postgres:password123@localhost:5499/etl_db")

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
        return luigi.LocalTarget("data/raw/electronics_products_pricing_data.csv")

    def run(self):
        pass

#luigi.build([ExtractDBAmazonSalesData(), ExtractAPIUniversitiesData()], local_scheduler = True)

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
        
# luigi.build([ValidateData()], local_scheduler = True)

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

        amazon_sales_data = amazon_sales_data.rename(columns = rename_cols)
        
        selected_columns = ["main_category", "sub_category", "ratings",
                            "number_of_ratings", "discount_price", "actual_price"]

        amazon_sales_data = amazon_sales_data[selected_columns]

        amazon_sales_data["discount_price"] = amazon_sales_data["discount_price"].str.replace("₹", " ")
        amazon_sales_data["actual_price"] = amazon_sales_data["actual_price"].str.replace("₹", " ")
        
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
            amazon_sales_data[column].fillna(median_value, inplace=True)

        validatation_process(data = amazon_sales_data,
                             table_name = "amazon_sales")

        amazon_sales_data.to_csv(self.output().path, index = False)
        

luigi.build([TransformAmazonSalesData()], local_scheduler = True)

