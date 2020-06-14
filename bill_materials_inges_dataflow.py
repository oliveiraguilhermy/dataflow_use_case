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
    line = {'tube_assembly_id':a[0],
            'component_id_1':a[1],
            'quantity_1':a[2],
            'component_id_2':a[3],
            'quantity_2':a[4],
            'component_id_3':a[5],
            'quantity_3':a[6],
            'component_id_4':a[7],
            'quantity_4':a[8],
            'component_id_5':a[9],
            'quantity_5':a[10],
            'component_id_6':a[11],
            'quantity_6':a[12],
            'component_id_7':a[13],
            'quantity_7':a[14],
            'component_id_8':a[15],
            'quantity_8':a[16]}
    return line

def run():
    schema = 'tube_assembly_id:STRING,component_id_1:STRING,quantity_1:FLOAT,component_id_2:STRING,quantity_2:FLOAT,component_id_3:STRING,quantity_3:FLOAT,component_id_4:STRING,quantity_4:FLOAT,component_id_5:STRING,quantity_5:FLOAT,component_id_6:STRING,quantity_6:FLOAT,component_id_7:STRING,quantity_7:FLOAT,component_id_8:STRING,quantity_8:FLOAT'
    argv = [
        f'--{PROJECT}',
        '--job_name=billofmaterials',
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
            | "read from GCS" >> beam.io.ReadFromText(f"gs://{BUCKET}/bill_of_materials.csv",skip_header_lines=1) 
            | "ParseCSV" >> beam.Map(lambda x : parse(x))
            | 'write_bq' >> beam.io.WriteToBigQuery(BQ_TABLE, dataset=BQ_DATASET_ID,schema=schema, write_disposition='WRITE_TRUNCATE')
            )
        p.run()
        log_import(arquivo="bill_of_materials.csv",dataset=BQ_DATASET_ID,tabela=BQ_TABLE,status="ok",timestamp=datetime.now(),obs='')
    except Exception as erro:
        log_import(arquivo="bill_of_materials.csv",dataset=BQ_DATASET_ID,tabela=BQ_TABLE,status="fail",timestamp=datetime.now(),obs=erro)

if __name__ == '__main__':
    run()