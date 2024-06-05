# Databricks notebook source
!pip install xlrd

# COMMAND ----------


from pathlib import Path
from dateutil.parser import parse
import pandas as pd
import re
catalog = "mimi_ws_1" # delta table destination catalog
schema = "grahamcenter" # delta table destination schema
tablename = "gme" # destination table
path = "/Volumes/mimi_ws_1/grahamcenter/src/gme/"

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.replace('#','num').lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

files = []
for filepath in Path(path).glob("*"):
    year = filepath.stem[4:]
    dt = parse(f"{year}-12-01").date()
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

pdf_lst = []
for item in files:
    pdf = pd.read_excel(item[1], dtype={"Provider Number": str}, na_values=".")
    pdf.columns = change_header(pdf.columns)
    pdf["fiscal_year_begin_date"] = pd.to_datetime(pdf["fiscal_year_begin_date"], 
                                                   format="%m/%d/%Y").dt.date
    pdf["fiscal_year_end_date"] = pd.to_datetime(pdf["fiscal_year_end_date"], 
                                                   format="%m/%d/%Y").dt.date
    pdf["_input_file_date"] = item[0]
    pdf_lst.append(pdf)

# COMMAND ----------

pdf_full = pd.concat(pdf_lst)
df = spark.createDataFrame(pdf_full)
(df.write
    .format('delta')
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------


