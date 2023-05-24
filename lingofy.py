import duckdb
import argparse
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import os
import json

def get_cardinality(parquet, name):
    query = f"SELECT COUNT(DISTINCT {name}) FROM parquet_scan('{parquet}')"
    con = duckdb.connect()
    result = con.execute(query).fetchall()
    return result[0][0]

def parquet_table_schema_to_json_columns(parquet, schema, rows=0):
   pks = []
   def map_field(field, pks):
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

        cardinality = get_cardinality(parquet, field.name)
        if cardinality == rows:
            pks.append(field.name)
        return {
            'name': field.name,
            'type': {
                'base': base,
                'props': props,
                'nullable': nullable
            },
             'distinct_values': cardinality  # This value needs to be calculated from the actual data
        }
   data = {
        'columns': [map_field(field, pks) for field in schema],
   }

   result = {
        "pkey": pks,
        "num_rows": rows,
        "columns": data['columns']
    }
   
   return result    

def parquet_schema(file_path):
    # Read the metadata of the Parquet file
    metadata = pq.read_metadata(file_path)
    
    # Get and print the schema
    schema = metadata.schema
    return (schema.to_arrow_schema(),metadata.num_rows)

def lingofy(input, output=None):
    if output is None:
        output = "lingodb"
    if not (os.path.exists(output) and os.path.isdir(output)):
       os.mkdir(output)
    
    
    file_root = os.path.basename(input).split(".")[0]
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

def get_parquet_files(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist")

    parquet_files = []

    if os.path.isfile(file_path):
        try:
            pq.ParquetFile(file_path)  # Test if it's a valid Parquet file
            return [os.path.basename(file_path)]
        except Exception:
            raise ValueError(f"{file_path} is not a valid Parquet file")

    elif os.path.isdir(file_path):
        for root, _, files in os.walk(file_path):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    try:
                        pq.ParquetFile(file_path)  # Test if it's a valid Parquet file
                        parquet_files.append(file_path)
                    except Exception:
                        pass

        if not parquet_files:
            raise ValueError(f"No Parquet files found in the directory: {file_path}")

        return parquet_files

    else:
        raise ValueError(f"{file_path} is neither a file nor a directory")

def generate_lingo_catalog(parquet_files):
    catalog = {'tables': {}}

    for file_path in parquet_files:
        arrow_schema, num_rows = parquet_schema(file_path)
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        json_columns = parquet_table_schema_to_json_columns(file_path, arrow_schema, num_rows)
        catalog['tables'][table_name] = json_columns

    return catalog

def main():
    parser = argparse.ArgumentParser(description="Process parquet input files and output a lingo database.")
    parser.add_argument("--input", type=str, required=True, help="Path to the parquet input file.")
    parser.add_argument("--output", type=str, required=False, help="Path to the output database directory name.")

    args = parser.parse_args()
    input = args.input
    output = args.output

    files = get_parquet_files(input)
    catalog = generate_lingo_catalog(files)
    
    progress_bar = tqdm(total=len(files), desc="Parquet -> Arrow ")

    for file in files:
        lingofy(file, output)
        progress_bar.update(1)

    progress_bar.close()
    if output is None:
        output = "lingodb"
    
    output_catalog_file = os.path.join(output, "metadata.json")
    
    with open(output_catalog_file, 'w') as f:
        json.dump(catalog, f, indent=2)

if __name__ == "__main__":
    main()