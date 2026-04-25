import duckdb
import os
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from urllib.parse import urlparse

# connect to DuckDB in-memory
con = duckdb.connect()

# load all CSV files from the data folder
data_folder = "/data"
tables = []

if os.path.exists(data_folder):
    for filename in os.listdir(data_folder):
        if filename.endswith(".csv"):
            table_name = filename.replace(".csv", "").replace("-", "_").replace(" ", "_")
            filepath = os.path.join(data_folder, filename)

            con.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{filepath}', header=True)
            """)

            tables.append(table_name)
            print(f"Loaded: {filename} -> table: {table_name}")
else:
    print(f"Warning: {data_folder} not found. No CSV files loaded.")

# build schema context
schema_context = ""

for table in tables:
    schema = con.execute(f"DESCRIBE {table}").fetchall()
    sample = con.execute(f"SELECT * FROM {table} LIMIT 3").fetchall()
    schema_context += f"\nTable: {table}\nSchema: {schema}\nSample rows: {sample}\n"

if tables:
    print("\nAll tables loaded:")
    print(schema_context)
else:
    print("\nNo tables loaded. Mount /data folder with CSV files.")

# HTTP server to query DuckDB
class DuckDBHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/tables':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'tables': tables}).encode())
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': 'Not found. Use GET /tables or POST /query'}).encode())
    
    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)
        
        try:
            data = json.loads(body)
            query = data.get('query', '')
            
            if not query:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'No query provided'}).encode())
                return
            
            result = con.execute(query).fetchall()
            columns = [desc[0] for desc in con.description] if con.description else []
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'columns': columns, 'data': result}).encode())
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())
    
    def log_message(self, format, *args):
        print(f"[DuckDB Server] {format % args}")

# Start HTTP server in background thread
server = HTTPServer(('0.0.0.0', 8000), DuckDBHandler)
print("\nDuckDB HTTP server starting on port 8000...")
thread = threading.Thread(target=server.serve_forever)
thread.daemon = False
thread.start()

print("Container running.")
print("  GET /tables - List all loaded tables")
print("  POST /query - Execute query with {\"query\": \"SELECT * FROM table_name\"}")
thread.join()
