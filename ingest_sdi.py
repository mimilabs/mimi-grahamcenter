# Databricks notebook source
from pathlib import Path
from dateutil.parser import parse
import pandas as pd
import re
catalog = "mimi_ws_1" # delta table destination catalog
schema = "grahamcenter" # delta table destination schema
tablename = "sdi" # destination table
path = "/Volumes/mimi_ws_1/grahamcenter/src/sdi/"

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.replace('#','num').lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

for level in ["censustract", "county", "pcsa", "zcta"]:
    pdf_lst = []
    for filepath in Path(path).glob(f"*-{level}.csv"):
        year = filepath.stem.split("-")[2][:4]
        ifd = parse(f"{year}-12-01").date()
        pdf = pd.read_csv(filepath, dtype={"CENSUSTRACT_FIPS": str, 
                                           "COUNTY_FIPS": str,
                                           "PCSA_FIPS": str,
                                           "ZCTA5_FIPS": str})
        pdf.columns = change_header(pdf.columns)
        pdf["_input_file_date"] = ifd
        pdf_lst.append(pdf)
    pdf_full = pd.concat(pdf_lst)
    df = spark.createDataFrame(pdf_full)
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}_{level}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.grahamcenter.sdi_county;
# MAGIC --DROP TABLE mimi_ws_1.grahamcenter.sdi_censustract;
# MAGIC --DROP TABLE mimi_ws_1.grahamcenter.sdi_pcsa;
# MAGIC --DROP TABLE mimi_ws_1.grahamcenter.sdi_zcta;
# MAGIC

# COMMAND ----------


