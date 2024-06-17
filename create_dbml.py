import sqlalchemy
from sqlalchemy import create_engine, MetaData
import psycopg2

# Define your PostgreSQL database connection parameters
DB_USER = 'postgres'
DB_PASSWORD = 'pass'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres'

# Create the connection string
DATABASE_URL = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Create an engine and connect to the database
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Reflect the existing database schema
metadata.reflect(bind=engine)

# Function to convert SQLAlchemy types to dbdiagram.io types
def convert_type(sqlalchemy_type):
    if isinstance(sqlalchemy_type, sqlalchemy.types.Integer):
        return 'int'
    elif isinstance(sqlalchemy_type, sqlalchemy.types.String):
        return 'varchar'
    elif isinstance(sqlalchemy_type, sqlalchemy.types.Float):
        return 'float'
    elif isinstance(sqlalchemy_type, sqlalchemy.types.Boolean):
        return 'boolean'
    elif isinstance(sqlalchemy_type, sqlalchemy.types.Date):
        return 'date'
    elif isinstance(sqlalchemy_type, sqlalchemy.types.DateTime):
        return 'datetime'
    elif isinstance(sqlalchemy_type, sqlalchemy.types.Time):
        return 'time'
    elif isinstance(sqlalchemy_type, sqlalchemy.types.Numeric):
        return 'decimal'
    else:
        return 'unknown'

# Function to write table definition to dbml format
def generate_dbml(table):
    dbml = f'Table {table.name} {{\n'
    for column in table.columns:
        column_type = convert_type(column.type)
        constraints = []
        if column.primary_key:
            constraints.append('pk')
        if column.unique:
            constraints.append('unique')
        if not column.nullable:
            constraints.append('not null')
        if column.default is not None:
            constraints.append(f'default: "{column.default}"')
        constraints_str = ', '.join(constraints)
        dbml += f'  {column.name} {column_type}'
        if constraints_str:
            dbml += f' [{constraints_str}]'
        dbml += '\n'
    dbml += '}\n'
    return dbml

# Generate dbml content for each table
dbml_content = ''
for table in metadata.tables.values():
    dbml_content += generate_dbml(table)
    dbml_content += '\n'

# Write the dbml content to a file
dbml_filename = 'database_schema.dbml'
with open(dbml_filename, 'w') as file:
    file.write(dbml_content)

print(f"Database schema has been written to '{dbml_filename}'")