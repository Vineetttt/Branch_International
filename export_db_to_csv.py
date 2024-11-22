import os
from sqlalchemy import create_engine
import pandas as pd
import 

# Database connection parameters
db_params = {
    'host': 'branchhomeworkdb.cv8nj4hg6yra.ap-south-1.rds.amazonaws.com',
    'port': 5432,
    'database': 'branchdsprojectgps',
    'user': 'datascientist',
    'password': '47eyYBLT0laW5j9U24Uuy8gLcrN'
}

connection_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"

# Function to export table to CSV
def export_table_to_csv(table_name, output_dir, engine):
    try:
        os.makedirs(output_dir, exist_ok=True)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, engine)
        csv_path = os.path.join(output_dir, f'{table_name}.csv')
        df.to_csv(csv_path, index=False)
        
        print(f"Exported {table_name} to {csv_path} successfully")
    except Exception as e:
        print(f"Error exporting {table_name}: {e}")

if __name__ == "__main__":
    engine = create_engine(connection_string)
    output_directory = os.path.join(os.getcwd(), "data")
    tables = ['loan_outcomes', 'gps_fixes', 'user_attributes']
    for table in tables:
        export_table_to_csv(table, output_directory, engine)
