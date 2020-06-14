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
    line = {'component_id':a[0],
            'component_type_id':a[1],
            'type':a[2],
            'connection_type_id':a[3],
            'outside_shape':a[4],
            'base_type':a[5],
            'height_over_tube':a[6],
            'bolt_pattern_long':a[7],
            'bolt_pattern_wide':a[8],
            'groove':a[9],
            'base_diameter':a[10],
            'shoulder_diameter':a[11],
            'unique_feature':a[12],
            'orientation':a[13],
            'weight':a[14]}
    return line

def run():
    schema = 'component_id:STRING,component_type_id:STRING,type:STRING,connection_type_id:STRING,outside_shape:STRING,base_type:STRING,height_over_tube:FLOAT,bolt_pattern_long:FLOAT,bolt_pattern_wide:FLOAT,groove:STRING,base_diameter:FLOAT,shoulder_diameter:FLOAT,unique_feature:STRING,orientation:STRING,weight:FLOAT'
    argv = [
        f'--{PROJECT}',
        '--job_name=compboss',
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
            | "read from GCS" >> beam.io.ReadFromText(f"gs://{BUCKET}/comp_boss.csv",skip_header_lines=1) 
            | "ParseCSV" >> beam.Map(lambda x : parse(x))
            | 'write_bq' >> beam.io.WriteToBigQuery(BQ_TABLE, dataset=BQ_DATASET_ID,schema=schema, write_disposition='WRITE_TRUNCATE')
            )
        p.run()
        log_import(arquivo="comp_boss.csv",dataset=BQ_DATASET_ID,tabela=BQ_TABLE,status="ok",timestamp=datetime.now(),obs='')
    except Exception as erro:
        log_import(arquivo="comp_boss.csv",dataset=BQ_DATASET_ID,tabela=BQ_TABLE,status="fail",timestamp=datetime.now(),obs=erro)

if __name__ == '__main__':
    run()