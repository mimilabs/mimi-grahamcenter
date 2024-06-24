# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

!pip install xlrd

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.grahamcenter.gme;

# COMMAND ----------

catalog = "mimi_ws_1" # delta table destination catalog
schema = "grahamcenter" # delta table destination schema
tablename = "gme" # destination table
path = f"/Volumes/mimi_ws_1/{schema}/src/{tablename}/"

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
    pdf = pdf.rename(columns={"num_of_dme_ft_es": "num_of_dme_ftes", 
                              "num_of_ime_ft_es": "num_of_ime_ftes"})
    pdf["fiscal_year_begin_date"] = pd.to_datetime(pdf["fiscal_year_begin_date"], 
                                                   format="%m/%d/%Y").dt.date
    pdf["fiscal_year_end_date"] = pd.to_datetime(pdf["fiscal_year_end_date"], 
                                                   format="%m/%d/%Y").dt.date
    pdf["mimi_src_file_date"] = item[0]
    pdf["mimi_src_file_name"] = item[1].name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
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


