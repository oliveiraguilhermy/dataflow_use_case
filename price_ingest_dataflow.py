import apache_beam as beam
import re
from google.cloud import bigquery
from datetime import datetime

#configs
BQ_DATASET_ID = 'DATASET' #input dataset
BQ_TABLE = 'price_quote'
BUCKET = 'Bucket_name' #input bucket name
PROJECT = 'PROJECT' #input project name
dataset_id = 'logs' #input log dataset
table_id = 'import_bq' #input table logs

#função de logs
def log_import(arquivo, dataset, tabela, status, timestamp, obs):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    table_log = client.get_table(table_ref)  

    row = [{
        "arquivo": arquivo,
        "dataset": dataset,
        "tabela": tabela,
        "status": status,
        "timestamp": datetime.timestamp(timestamp),
        "obs": str(obs)
    }]
    client.insert_rows_json(table=table_log, json_rows=row)


def parse(row):
    a = re.split(",",re.sub('\r\n', '', re.sub(u'"', '', row)))
    line={
        'tube_assembly_id':a[0],
        'supplier':a[1],
        'quote_date':a[2],
        'annual_usage':a[3],
        'min_order_quantity':a[4],
        'bracket_pricing':a[5],
        'quantity':a[6],
        'cost':a[7]}
    return line

def run():
    schema = 'tube_assembly_id:STRING,supplier:STRING,quote_date:STRING,annual_usage:INT64,min_order_quantity:INT64,bracket_pricing:STRING,quantity:INT64,cost:FLOAT'
    argv = [
        f'--{PROJECT}',
        '--job_name=pricequote',
        '--save_main_session',
        f'--staging_location=gs://{BUCKET}/',
        f'--temp_location=gs://{BUCKET}/',
        '--runner=DataflowRunner',
        '--disk_size_gb=30',
        '--region=us-central1',
        '--num_workers=1',
        '--max_num_workers=1',
        '--machine_type=n1-standard-1'
    ]

    try:
        p = beam.Pipeline(argv=argv)
        (p
            | "read from GCS" >> beam.io.ReadFromText(f"gs://{BUCKET}/price_quote.csv",skip_header_lines=1) 
            | "ParseCSV" >> beam.Map(lambda x : parse(x))
            | 'write_bq' >> beam.io.WriteToBigQuery(BQ_TABLE, dataset=BQ_DATASET_ID,schema=schema, write_disposition='WRITE_TRUNCATE')
            )
        p.run()
        log_import(arquivo="price_quote.csv",dataset=BQ_DATASET_ID,tabela=BQ_TABLE,status="ok",timestamp=datetime.now(),obs='')
    except Exception as erro:
        log_import(arquivo="price_quote.csv",dataset=BQ_DATASET_ID,tabela=BQ_TABLE,status="fail",timestamp=datetime.now(),obs=erro)

if __name__ == '__main__':
    run()