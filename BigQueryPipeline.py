import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.transforms.sql import SqlTransform

#import apache_beam.sql
import apache_beam.transforms.sql
from apache_beam.transforms.util import BatchElements
from apache_beam import pvalue
import itertools
import time

payment_type_name_map = {1:'Credit Card', 2:'Cash', 3:'No charge', 
                         4:'Dispute', 5:'Unknown',  6:'Voidedtrip'}

ratecode_name_map = {1:'Standard rate', 2:'JFK', 3:'Newark',
                     4:'Nassau or Westchester', 5:'Negotiated fare', 6:'Group ride'}

payment_type_dim_schema = '''payment_type_ID:INTEGER, payment_type:INTEGER, payment_type_name:STRING'''

datetime_dim_schema = '''datetime_index:INTEGER, tpep_pickup_datetime:DATETIME, PU_year:INTEGER, PU_month:INTEGER, PU_day:INTEGER,
                        PU_hour:INTEGER, tpep_dropoff_datetime:DATETIME, DO_year:INTEGER, DO_month:INTEGER, DO_day:INTEGER, DO_hour:INTEGER'''

pickup_location_dim_schema = '''PU_ID:INTEGER, PU_locationID:INTEGER, PU_borough:STRING, PU_zone:STRING, PU_service_zone:STRING'''

# fact_table_schema = '''trip_ID:INTEGER, vendo_ID:INTEGER, passenger_count:INTEGER, '''

def create_initial_payment_type_dim(row):   
    payment_type_ID = row['index']
    payment_type = row['payment_type']

    new_row = {'payment_type_ID' : payment_type_ID,
                   'payment_type' : payment_type
                #    'payment_type_name': payment_type_name_map.get(payment_type, '')
                }
        
    return new_row

def create_payment_type_dim(row, payment_type_name_map):
    row['payment_type_name'] = payment_type_name_map.get(row['payment_type'], '')
    return row

def create_datetime_dim(row):
    datetime_ID = int(row['index'])
    tpep_pickup_datetime = row['tpep_pickup_datetime']
    PU_year = tpep_pickup_datetime.year
    PU_month = tpep_pickup_datetime.month
    PU_day = tpep_pickup_datetime.day
    PU_hour = tpep_pickup_datetime.hour
    tpep_dropoff_datetime = row['tpep_dropoff_datetime']
    DO_year = tpep_dropoff_datetime.year
    DO_month = tpep_dropoff_datetime.month
    DO_day = tpep_dropoff_datetime.day
    DO_hour = tpep_dropoff_datetime.hour

    new_row = {'datetime_ID' : datetime_ID,
                'tpep_pickup_datetime' : tpep_pickup_datetime,
                'PU_year' : PU_year,
                'PU_day' : PU_day,
                'PU_month' : PU_month,
                'PU_hour' : PU_hour,
                'tpep_dropoff_datetime' : tpep_dropoff_datetime,
                'DO_year' : DO_year,
                'DO_month' : DO_month,
                'DO_day' : DO_day,
                'DO_hour' : DO_hour
                }
        
    return new_row 

def create_initial_ratecode_dim(row):
    rate_code_ID = row['index']
    rate_code = row['RatecodeID']

    new_row = {'rate_code_ID' : rate_code_ID,
               'rate_code' : rate_code
               }
    
    return new_row

def create_ratecode_dim(row, ratecode_name_map):
    row['rate_code_name'] = ratecode_name_map.get(row['rate_code'], '')

    return row

def create_cost_dim(row):
    cost_ID = row['index']
    fare_amount = row['fare_amount']
    tolls_amount = row['tolls_amount']
    airport_fee = row['airport_fee']
    extra = row['extra']
    improvement_surcharge = row['improvement_surcharge']
    congestion_surcharge = row['congestion_surcharge']
    tip_amount = row['tip_amount']
    total_amount = row['total_amount']

    new_row = {'cost_ID' : cost_ID, 'fare_amount' : fare_amount,
               'tolls_amount' :  tolls_amount, 'airport_fee' : airport_fee, 
               'extra' : extra, 'improvement_surcharge' : improvement_surcharge, 
               'congestion_surcharge' : congestion_surcharge,
               'tip_amount' : tip_amount, 'total_amount' : total_amount}

    return new_row

def create_initial_pickup_location_dim(row):

    ID = row['index']
    LocationID = row['PULocationID']

    new_row = {'PU_ID' : ID, 'PU_LocationID' : LocationID}

    return new_row

def create_initial_dropoff_location_dim(row):

    ID = row['index']
    LocationID = row['DOLocationID']
    
    new_row = {'DO_ID' : ID, 'DO_LocationID' : LocationID}

    return new_row

def create_fact_table(row):

    # if 'passenger_count' in row:
    #     if row['passenger_count'] is not None:
    #         try:
    #             row['passenger_count'] = int(row['passenger_count'])
    #         except ValueError:
    #             row['passenger_count'] = None

    trip_ID = row['index']
    vendor_ID = row['VendorID']
    datetime_ID = row['index']
    passenger_count = row['passenger_count']
    trip_distance = row['trip_distance']
    rate_code_ID = row['index']
    store_and_fwd_flag = row['store_and_fwd_flag']
    PU_ID = row['index']
    DO_ID = row['index']
    payment_type_ID = row['index']
    cost_ID = row['index']


    new_row = {'trip_ID' : trip_ID, 'vendorID' : vendor_ID, 'datetime_ID' : datetime_ID,
               'passenger_count' : passenger_count, 'trip_distance' : trip_distance,
               'rate_code_ID' : rate_code_ID, 'store_and_fwd_flag' : store_and_fwd_flag,
               'PU_ID' : PU_ID, 'DO_ID' : DO_ID, 'payment_type_ID' : payment_type_ID,
               'cost_ID' : cost_ID}

    return new_row

# def map_payment_type(row):
#     row['payment_type_name'] = payment_type_name_map.get(row['payment_type'], 'Unknown')
#     return row

     
def run_pipeline(project_id, bucket_name, input_path, output_table, batch_number):
    
    options = beam.pipeline.PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        region='us-west1'  
    )


    with beam.Pipeline(options=options) as p:
    # Read parquet data
        data = (p | 'ReadParquet' >> beam.io.ReadFromParquet(f'gs://{bucket_name}/{input_path}/batch_number={batch_number}/*.parquet'))

        datetime_dim = (data | 'CreateDateTimeDim' >> beam.Map(create_datetime_dim))

        datetime_dim | 'WriteDateTimeDimToBigQuery' >> beam.io.WriteToBigQuery(
            table=f'''{output_table}.datetime_dim''',
            schema='''datetime_ID:INTEGER, tpep_pickup_datetime:DATETIME, PU_year:INTEGER, PU_month:INTEGER, PU_day:INTEGER,
                    PU_hour:INTEGER, tpep_dropoff_datetime:DATETIME, DO_year:INTEGER, DO_month:INTEGER, DO_day:INTEGER, DO_hour:INTEGER''',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        initial_payment_type_dim = data | 'CreateInitialPaymentTypeDim' >> beam.Map(create_initial_payment_type_dim)

        payment_type_dim = initial_payment_type_dim | 'CreatePaymentTypeDim' >> beam.Map(create_payment_type_dim, payment_type_name_map)

        payment_type_dim | 'WritePaymentTypeDimToBQ' >> beam.io.WriteToBigQuery(
            table= f'''{output_table}.payment_type_dim''',
            schema='''SCHEMA_AUTODETECT''',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        initial_ratecode_dim = data | 'CreateInitialRateCodeDim' >> beam.Map(create_initial_ratecode_dim)

        ratecode_dim = initial_ratecode_dim |  'CreateRateCodeDim' >> beam.Map(create_ratecode_dim, ratecode_name_map)

        ratecode_dim | 'WriteRateCodeDimToBQ' >>  beam.io.WriteToBigQuery(
            table = f'''{output_table}.ratecode_dim''',
            schema='''rate_code_ID:INTEGER,rate_code:FLOAT,rate_code_name:STRING''',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        pickup_location_dim  = data | 'CreatePickupLocationDim' >> beam.Map(create_initial_pickup_location_dim)

        pickup_location_dim | 'WritePULocationDimToBQ' >> beam.io.WriteToBigQuery(
            table = f'''{output_table}.pickup_location_dim''',
            schema="PU_ID:INTEGER,PU_LocationID:INTEGER",
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        dropoff_location_dim  = data | 'CreateDropoffLocationDim' >> beam.Map(create_initial_dropoff_location_dim)

        dropoff_location_dim | 'WriteDOLocationDimToBQ' >> beam.io.WriteToBigQuery(
            table = f'''{output_table}.dropoff_location_dim''',
            schema="DO_ID:INTEGER,DO_LocationID:INTEGER",
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        # fact_table = data | 'CreateFactTable'  >> beam.Map(create_fact_table)

        # fact_table | 'WriteFactTableToBQ' >> beam.io.WriteToBigQuery(
        #     table =  f'''{output_table}.fact_table''',
        #     schema= '''SCHEMA_AUTODETECT''',
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        # )

        cost_dim =  data | 'CreateCostDim'  >> beam.Map(create_cost_dim)

        cost_dim  | 'WriteCostDimToBQ' >> beam.io.WriteToBigQuery(
            table = f'''{output_table}.cost_dim''',
            schema='''SCHEMA_AUTODETECT''',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

    
## Batch process data TO DO

# run_pipeline('nyc-taxi-project-423502', 'pipeline_test_pc', 'data_test1', 'nyc_pipeline_test')

batch_numbers = [0,1,2,3,4,5,6,7,8,9,10,11]


for batch_number in batch_numbers:
    print(f'''starting batch {batch_number}''')
    # total_time_taken = 0
    # average_batch_time = 0
    # length = len(batch_numbers)
    # max_minutes  = 0
    # max_seconds = 0
    try:
        start_time = time.time()
        run_pipeline('nyc-yellow-taxi-data-project', 'nyc_yellow_taxi_project', 'transformed_nyc_yellow_taxi_data', 'initial_testing_pipeline_upload',batch_number)
        end_time = time.time()
        batch_time = end_time - start_time
        minutes, seconds = divmod(batch_time, 60)
        print(f'''batch {batch_number} successfully completed''')
        print(f'''batch {batch_number} took {minutes} minutes {seconds} seconds''')
        # total_time_taken += batch_time
        # if  (minutes >= max_minutes) & (seconds > max_seconds):
        #     max_minutes = minutes
        #     max_seconds = seconds

    except Exception as e:
        print(f'''batch {batch_number} failed: {e}''')
        break

# total_hours, remainder = divmod(total_time_taken, 3600)
# total_minutes, total_seconds = divmod(remainder, 60)
# avg_minutes, avg_seconds = divmod(total_time_taken, 60)
# print(f'''all batches took {total_hours} hours {total_minutes} minutes {total_seconds} seconds''')
# print(f'''average batch time was {avg_minutes} minutes {avg_seconds} seconds''')

