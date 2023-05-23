import duckdb
import argparse
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json

def parquet_table_schema_to_json_columns(schema, table_name, rows=0, pk=""):
   def map_field(field):
        field_type = field.type
        nullable = field.nullable
        base = None
        props = []
        if pa.uint32().equals(field_type) or pa.uint64().equals(field_type):
            base = 'int'
            props = [str(field_type.bit_width)]
        if pa.int32().equals(field_type) or pa.int64().equals(field_type):
            base = 'int'
            props = [str(field_type.bit_width)]
        elif pa.float32().equals(field_type) or pa.float64().equals(field_type):
            base = 'float'
            props = []
        elif pa.bool_().equals(field_type):
            base = 'bool'
            props = ['i1']
        elif pa.date32().equals(field_type) or pa.date64().equals(field_type):
            base = 'Date'
            props = []
        elif pa.timestamp('ms').equals(field_type) or pa.timestamp('us').equals(field_type) or pa.timestamp('ns').equals(field_type):
            base = 'Timestamp'
            props = []
        elif pa.string().equals(field_type):
            base = 'String'
            props = []

        return {
            'name': field.name,
            'type': {
                'base': base,
                'props': props,
                'nullable': nullable
            }
            #'distinct_values': 0  # This value needs to be calculated from the actual data
        }
   data = {
        'columns': [map_field(field) for field in schema],
   }
   result = f"""{{ "{table_name}":"pkey":"{pk}","num_rows":{rows},{json.dumps(data)} 
    }}"""
   print(result)    

def parquet_schema(file_path):
    # Read the metadata of the Parquet file
    metadata = pq.read_metadata(file_path)
    
    # Get and print the schema
    schema = metadata.schema
    return (schema.to_arrow_schema(),metadata.num_rows)

def lingofy(input, output):
    if output is None:
        output = "lingodb"
    if os.path.exists(output) and os.path.isdir(output):
        raise Exception("Output directory already exists.")
    else:
       os.mkdir(output)
    
    file_root, _ = os.path.splitext(input)
    lingofile = f"{file_root}.arrow"
    dbpath = os.path.join(output, lingofile)
    chunk_size = 1_000

    query = f"SELECT * FROM parquet_scan('{input}')"
    con = duckdb.connect()
    result = con.execute(query).arrow(chunk_size)
    
    schema = result.schema
    sink = pa.OSFile(dbpath, "wb")
    writer = pa.RecordBatchFileWriter(sink, schema)

    writer.write_table(result)
    writer.close()
    sink.close()
        

def main():
    parser = argparse.ArgumentParser(description="Process parquet input files and output a lingo database.")
    parser.add_argument("--input", type=str, required=True, help="Path to the parquet input file.")
    parser.add_argument("--output", type=str, required=False, help="Path to the output database directory name.")

    args = parser.parse_args()
    (table_schema,rows) = parquet_schema(args.input)
    parquet_table_schema_to_json_columns(table_schema, "test", rows, "device_sk")
    #lingofy(args.input, args.output)

if __name__ == "__main__":
    main()