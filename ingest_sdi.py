# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

catalog = "mimi_ws_1" # delta table destination catalog
schema = "grahamcenter" # delta table destination schema
tablename = "sdi" # destination table
path = f"/Volumes/mimi_ws_1/{schema}/src/{tablename}/"

# COMMAND ----------

for level in ["censustract", "county", "pcsa", "zcta"]:
    pdf_lst = []
    for filepath in Path(path).glob(f"*-{level}.csv"):        
        pdf = pd.read_csv(filepath, dtype={"CENSUSTRACT_FIPS": str, 
                                           "COUNTY_FIPS": str,
                                           "PCSA_FIPS": str,
                                           "ZCTA5_FIPS": str})
        if "CENSUSTRACT_FIPS" in pdf.columns:
            pdf['CENSUSTRACT_FIPS'] = pdf['CENSUSTRACT_FIPS'].str.zfill(11)
        elif "COUNTY_FIPS" in pdf.columns:
            pdf['COUNTY_FIPS'] = pdf['COUNTY_FIPS'].str.zfill(5)
        elif "PCSA_FIPS" in pdf.columns:
            pdf['PCSA_FIPS'] = pdf['PCSA_FIPS'].str.zfill(11)
        elif "ZCTA5_FIPS" in pdf.columns:
            pdf['ZCTA5_FIPS'] = pdf['ZCTA5_FIPS'].str.zfill(5)

        pdf.columns = change_header(pdf.columns)
        year = filepath.stem.split("-")[2][:4]
        pdf["mimi_src_file_date"] = parse(f"{year}-12-01").date()
        pdf["mimi_src_file_name"] = filepath.name
        pdf["mimi_dlt_load_date"] = datetime.today().date()
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


