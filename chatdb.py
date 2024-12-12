from flask import Flask, request, jsonify
from flask import render_template
import pandas as pd
import mysql.connector
import os
import random

app = Flask(__name__)

UPLOAD_FOLDER = './uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def get_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='hanhuawei',
        database='chatdb'
    )

@app.route('/')
def index():
    instructions = """
    <h1>Welcome to ChatDB!</h1>
    <p>Use the following links and forms to interact with ChatDB:</p>
    <ul>
        <li>
            <strong><a href="/upload">Upload a Dataset</a></strong>
            <p>Upload your dataset in CSV format to a specified table.</p>
        </li>
        <li>
            <strong>List Tables in a Database</strong>
            <form action="/tables" method="get">
                <label for="database">Enter Database Name:</label>
                <input type="text" id="database" name="database" required>
                <button type="submit">View Tables</button>
            </form>
        </li>
        <li>
            <strong>Get Table Info</strong>
            <form action="/table_info" method="get">
                <label for="database">Enter Database Name:</label>
                <input type="text" id="database" name="database" required><br><br>
                <label for="table">Enter Table Name:</label>
                <input type="text" id="table" name="table" required>
                <button type="submit">View Table Info</button>
            </form>
        </li>
        <li>
            <strong>Interactive Chat Interface</strong>
            <form action="/chat" method="get">
                <label for="database">Enter Database Name:</label>
                <input type="text" id="database" name="database" required>
                <button type="submit">Start Chat</button>
            </form>
        </li>
        <li><a href="/natural_language_query">Natural Language Query</a></li>
    </ul>
    <p>Enjoy using ChatDB!</p>
    """
    return instructions




@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'GET':
        return '''
            <form action="/upload" method="post" enctype="multipart/form-data">
                <label for="table_name">Table Name:</label>
                <input type="text" id="table_name" name="table_name" required><br><br>
                <label for="file">Choose CSV File:</label>
                <input type="file" id="file" name="file" accept=".csv" required><br><br>
                <button type="submit">Upload</button>
            </form>
        '''
    elif request.method == 'POST':
        if 'file' not in request.files:
            return jsonify({'error': 'No file part in the request.'}), 400

        file = request.files['file']
        table_name = request.form['table_name']

        if file.filename == '':
            return jsonify({'error': 'No selected file.'}), 400

        if file and file.filename.endswith('.csv'):
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(file_path)
            df = pd.read_csv(file_path)

            # Log file saving
            print(f"File saved to {file_path}")

            # Read the CSV file
            df = pd.read_csv(file_path)
            print(f"CSV loaded with shape: {df.shape}")
            df = df.where(pd.notnull(df), None)

            # Connect to the MySQL database
            conn = mysql.connector.connect(
                host='localhost',
                user='root',
                password='hanhuawei',
                database='chatdb'
            )
            cursor = conn.cursor()

            # Create table dynamically
            df.columns = df.columns.str.replace(' ', '_')
            create_table_query = generate_create_table_query(table_name, df)
            print(f"Executing: {create_table_query}")
            cursor.execute(create_table_query)

            # Insert data into the table
            for _, row in df.iterrows():
                placeholders = ', '.join(['%s'] * len(row))
                insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                cursor.execute(insert_query, tuple(row))

            conn.commit()
            print(f"Data inserted into {table_name} successfully")
            return jsonify({'message': f'Dataset uploaded and inserted into {table_name} successfully.'})
        else:
            return jsonify({'error': 'Unsupported file type. Only .csv is allowed.'}), 400



def generate_create_table_query(table_name, df):
    columns = []
    for column_name, dtype in zip(df.columns, df.dtypes):
        if 'int' in str(dtype):
            col_type = 'INT'
        elif 'float' in str(dtype):
            col_type = 'DECIMAL(10,2)'
        elif 'datetime' in str(dtype):
            col_type = 'DATETIME'
        else:
            col_type = 'VARCHAR(255)'
        columns.append(f"`{column_name}` {col_type}")
    columns_str = ', '.join(columns)
    return f"CREATE TABLE `{table_name}` ({columns_str});"

# List all databases
@app.route('/databases', methods=['GET'])
def list_databases():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES")
    databases = [db[0] for db in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify({'databases': databases})

# List tables in a selected database
@app.route('/tables', methods=['GET'])
def list_tables():
    database = request.args.get('database')
    if not database:
        return jsonify({'error': 'Database not specified'}), 400

    conn = get_connection()
    conn.database = database 
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify({'database': database, 'tables': tables})

@app.route('/table_info', methods=['GET'])
def table_info():
    database = request.args.get('database')
    table = request.args.get('table')
    if not database or not table:
        return "<h3>Error: Database or table not specified.</h3>", 400

    conn = get_connection()
    conn.database = database  
    cursor = conn.cursor()

    # Get table attributes
    cursor.execute(f"DESCRIBE {table}")
    attributes = cursor.fetchall()

    # Get sample data
    cursor.execute(f"SELECT * FROM {table} LIMIT 5")
    sample_data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]  # Extract column names

    cursor.close()
    conn.close()

    # Render as HTML table
    return render_template('table_info.html', 
                           database=database, 
                           table=table, 
                           attributes=attributes, 
                           sample_data=sample_data, 
                           column_names=column_names)





@app.route('/chat', methods=['GET', 'POST'])
def chat():
    if request.method == 'GET':
        database = request.args.get('database')
        if not database:
            return "<h3>Error: Database not specified.</h3><p><a href='/'>Back to Home</a></p>", 400

        print(f"Debug: /chat - database={database}")

        return f"""
        <h1>ChatDB Interactive Interface</h1>
        <form action="/chat" method="post">
            <input type="hidden" name="database" value="{database}">
            <label for="user_input">Ask your question:</label><br>
            <textarea id="user_input" name="user_input" rows="4" cols="50" placeholder="E.g., example query with group by"></textarea><br><br>
            <button type="submit">Submit</button>
        </form>
        <p><a href="/">Back to Home</a></p>
        """

    elif request.method == 'POST':
        user_input = request.form.get('user_input', '').lower()
        database = request.form.get('database')

        if not database or database == "{database}":
            return "<h3>Error: Invalid or missing database.</h3><p><a href='/'>Back to Home</a></p>", 400

        # Check for "example" in user input
        if "example" in user_input:
            sql_commands = [
                "select", "group by", "where", "join", "max", "avg", "min", "count", "order by", "sum", "distinct"
            ]
            command_detected = None
            for command in sql_commands:
                if command in user_input:
                    command_detected = command
                    break

            if command_detected:
                queries = generate_command_query(user_input)
                if queries:
                    html = f"""
                    <h1>ChatDB Interactive Interface</h1>
                    <h3>Question: {user_input}</h3>
                    <h4>Generated Example Queries:</h4>
                    <ol>
                    """
                    for query in queries:
                        html += f"""
                        <li>
                            <pre>{query}</pre>
                            <form action="/execute_query" method="post" style="display:inline;">
                                <input type="hidden" name="database" value="{database}">
                                <input type="hidden" name="query" value="{query}">
                                <button type="submit">Execute</button>
                            </form>
                        </li>
                        """
                    html += "</ol><p><a href='/chat?database={database}'>Ask Another Question</a></p>"
                    html += "<p><a href='/'>Back to Home</a></p>"
                    return html
            else:
                queries = generate_example_queries()  
                html = f"""
                <h1>ChatDB Interactive Interface</h1>
                <h3>Question: {user_input}</h3>
                <h4>Generated Example Queries:</h4>
                <ol>
                """
                for query in queries:
                    html += f"""
                    <li>
                        <pre>{query}</pre>
                        <form action="/execute_query" method="post" style="display:inline;">
                            <input type="hidden" name="database" value="{database}">
                            <input type="hidden" name="query" value="{query}">
                            <button type="submit">Execute</button>
                        </form>
                    </li>
                    """
                html += "</ol><p><a href='/chat?database={database}'>Ask Another Question</a></p>"
                html += "<p><a href='/'>Back to Home</a></p>"
                return html

        query, description = generate_sql_query(user_input)

        if query:
            return f"""
            <h1>ChatDB Interactive Interface</h1>
            <h3>Question: {user_input}</h3>
            <h4>Description: {description}</h4>
            <h4>Generated SQL Query:</h4>
            <pre>{query}</pre>
            <form action="/execute_query" method="post">
                <input type="hidden" name="database" value="{database}">
                <input type="hidden" name="query" value="{query}">
                <button type="submit">Execute Query</button>
            </form>
            <p><a href="/chat?database={database}">Ask Another Question</a></p>
            <p><a href="/">Back to Home</a></p>
            """
        else:
            return f"<h3>No matching pattern found for: {user_input}</h3><p><a href='/chat?database={database}'>Try Again</a></p>"



@app.route('/execute_query', methods=['POST'])
def execute_query():
    database = request.form.get('database')
    query = request.form.get('query')

    if not database or database == "{database}":
        return "<h3>Error: Invalid or missing database.</h3><p><a href='/'>Back to Home</a></p>", 400

    print(f"Debug: /execute_query - database={database}, query={query}")

    conn = None
    cursor = None

    try:
        # Handle tuple queries: extract only the SQL query part
        if query.startswith("("):
            query = eval(query)[0]

        conn = get_connection()
        conn.database = database  
        cursor = conn.cursor()

        cursor.execute(query)
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        return f"""
        <h1>ChatDB Query Execution</h1>
        <h4>Executed Query:</h4>
        <pre>{query}</pre>
        <h4>Query Results:</h4>
        <table border="1">
            <thead>
                <tr>{''.join(f'<th>{col}</th>' for col in column_names)}</tr>
            </thead>
            <tbody>
                {''.join('<tr>' + ''.join(f'<td>{val}</td>' for val in row) + '</tr>' for row in results)}
            </tbody>
        </table>
        <p><a href="/chat?database={database}">Back to Interactive Chat Interface</a></p>
        <p><a href="/natural_language_query">Back to NLQ</a></p>
        <p><a href="/">Back to Home</a></p>
        """
    except mysql.connector.errors.ProgrammingError as e:
        return f"<h3>Error: {str(e)}</h3><p><a href='/chat?database={database}'>Back</a></p>"
    except Exception as e:
        return f"<h3>Unexpected Error: {str(e)}</h3><p><a href='/chat?database={database}'>Back</a></p>"
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()






def generate_sql_query(user_input):
    # Define case-insensitive SQL keywords and patterns
    sql_keywords = [
        "select", "distinct", "where", "order by", "and", "or", "not", 
        "min", "max", "sum", "count", "avg", "join", "group by"
    ]

    # Convert user input to lowercase for case-insensitive matching
    user_input_lower = user_input.lower()

    # Check if user is requesting examples without specific commands
    if "example" in user_input_lower and not any(keyword in user_input_lower for keyword in sql_keywords):
        return generate_example_queries(), "Example queries covering diverse SQL commands"

    # Check for specific commands in the user input
    for keyword in sql_keywords:
        if keyword in user_input_lower:
            return generate_command_query(keyword), f"Example query with '{keyword.upper()}'"

    # No matching pattern
    return None, None

def generate_command_query(user_input):
    database = "chatdb"  # Replace with the actual database name
    tables = ["Products", "Sales", "Customers"]  # Replace with actual tables

    # SQL commands to detect
    sql_commands = [
        "select", "group by", "where", "join", "max", "avg", "min", "count", "order by", "sum", "distinct"
    ]

    # Parse user input
    user_input_lower = user_input.lower()
    if "example" in user_input_lower:
        for command in sql_commands:
            if command in user_input_lower:
                return generate_examples_for_command(command, database, tables)

    # If no specific command is found, return None
    return None



def get_attribute_types(database, table):
    """
    Fetch attribute types for the given table.
    """
    conn = get_connection()
    conn.database = database
    cursor = conn.cursor()

    cursor.execute(f"DESCRIBE {table}")
    schema = cursor.fetchall()

    numerical = []
    non_numerical = []

    for col in schema:
        field, dtype = col[0], col[1]
        if any(keyword in dtype.lower() for keyword in ['int', 'decimal', 'float', 'double']):
            numerical.append(field)
        else:
            non_numerical.append(field)

    cursor.close()
    conn.close()
    return numerical, non_numerical


def generate_example_queries():
    database = "chatdb"  # Replace with the actual database name
    tables = ["Products", "Sales", "Customers"]  # Replace with actual tables
    example_queries = []

    for _ in range(5):
        # Randomly select a table and fetch its attributes
        table = random.choice(tables)
        numerical, non_numerical = get_attribute_types(database, table)

        # Define example query templates
        query_templates = []

        # Queries for numerical attributes
        if numerical:
            query_templates += [
    # Aggregation queries
    (f"SELECT MAX({attr}) FROM {table};", f"Find the maximum value of {attr} in the {table} table.") for attr in numerical
] + [
    (f"SELECT MIN({attr}) FROM {table};", f"Find the minimum value of {attr} in the {table} table.") for attr in numerical
] + [
    (f"SELECT AVG({attr}) FROM {table};", f"Find the average value of {attr} in the {table} table.") for attr in numerical
] + [
    (f"SELECT SUM({attr}) FROM {table};", f"Calculate the total sum of {attr} in the {table} table.") for attr in numerical
] + [
    (f"SELECT COUNT({attr}) FROM {table};", f"Count the number of entries in the {attr} column of the {table} table.") for attr in numerical
] + [

    # GROUP BY with aggregation
    (f"SELECT {attr}, SUM({attr}) FROM {table} GROUP BY {attr};", 
     f"Group by {attr} and calculate the sum for each group in the {table} table.") for attr in numerical
] + [
    (f"SELECT {attr}, AVG({attr}) FROM {table} GROUP BY {attr};", 
     f"Group by {attr} and calculate the average for each group in the {table} table.") for attr in numerical
] + [

    # WHERE conditions
    (f"SELECT {attr} FROM {table} WHERE {attr} > 100;", 
     f"Find rows where {attr} is greater than 100 in the {table} table.") for attr in numerical
] + [
    (f"SELECT {attr} FROM {table} WHERE {attr} < 50;", 
     f"Find rows where {attr} is less than 50 in the {table} table.") for attr in numerical
] + [
    (f"SELECT {attr} FROM {table} WHERE {attr} BETWEEN 10 AND 100;", 
     f"Find rows where {attr} is between 10 and 100 in the {table} table.") for attr in numerical
] + [

    # ORDER BY queries
    (f"SELECT {attr} FROM {table} ORDER BY {attr} DESC LIMIT 5;", 
     f"Retrieve the top 5 values of {attr} from the {table} table in descending order.") for attr in numerical
] + [
    (f"SELECT {attr} FROM {table} ORDER BY {attr} ASC LIMIT 10;", 
     f"Retrieve the top 10 values of {attr} from the {table} table in ascending order.") for attr in numerical
] + [

    # JOIN queries involving numerical attributes
    (f"SELECT t1.{attr}, t2.{attr} FROM {table} t1 JOIN {table} t2 ON t1.{attr} = t2.{attr};", 
     f"Perform a self-join on the {table} table using the {attr} column.") for attr in numerical
] + [

    # Mathematical expressions
    (f"SELECT {attr}, {attr} * 2 AS Double_{attr} FROM {table};", 
     f"Retrieve {attr} and calculate its double in the {table} table.") for attr in numerical
] + [
    (f"SELECT {attr}, {attr} / 2 AS Half_{attr} FROM {table};", 
     f"Retrieve {attr} and calculate its half in the {table} table.") for attr in numerical
] + [
    (f"SELECT {attr}, {attr} + 100 AS Incremented_{attr} FROM {table};", 
     f"Retrieve {attr} and increment it by 100 in the {table} table.") for attr in numerical
] + [
    (f"SELECT {attr}, {attr} - 10 AS Decremented_{attr} FROM {table};", 
     f"Retrieve {attr} and decrement it by 10 in the {table} table.") for attr in numerical
]


        
        if query_templates:
            example_queries.append(random.choice(query_templates))

    return example_queries


def generate_examples_for_command(command, database, tables):
    table = random.choice(tables)  
    numerical, non_numerical = get_attribute_types(database, table)

    command_patterns = {
    "select": lambda: [
        (
            f"SELECT {random.choice(numerical + non_numerical)} FROM {table} LIMIT 5;",
            f"Select the first 5 rows of {random.choice(numerical + non_numerical)} from the {table} table."
        ),
        (
            f"SELECT {random.choice(numerical + non_numerical)} FROM {table} WHERE {random.choice(numerical)} > 50 LIMIT 5;",
            f"Select the first 5 rows of {random.choice(numerical + non_numerical)} where {random.choice(numerical)} is greater than 50 in the {table} table."
        ),
        (
            f"SELECT {', '.join(random.sample(numerical + non_numerical, 2))} FROM {table} LIMIT 10;",
            f"Select the first 10 rows of two attributes ({', '.join(random.sample(numerical + non_numerical, 2))}) from the {table} table."
        )
    ] if numerical + non_numerical else [],

    "group by": lambda: [
        (
            f"SELECT {non_agg}, COUNT(*) FROM {table} GROUP BY {non_agg};",
            f"Group by {non_agg} and count the number of occurrences in the {table} table."
        ) for non_agg in non_numerical
    ] + [
        (
            f"SELECT {non_agg}, {random.choice(numerical)}, AVG({random.choice(numerical)}) FROM {table} GROUP BY {non_agg}, {random.choice(numerical)};",
            f"Group by {non_agg} and {random.choice(numerical)} to calculate the average of {random.choice(numerical)} in the {table} table."
        ) for non_agg in non_numerical
    ] if non_numerical and numerical else [],

    "where": lambda: [
        (
            f"SELECT {random.choice(numerical)} FROM {table} WHERE {random.choice(numerical)} > 100;",
            f"Find rows where {random.choice(numerical)} is greater than 100 in the {table} table."
        ),
        (
            f"SELECT {random.choice(numerical)} FROM {table} WHERE {random.choice(numerical)} BETWEEN 10 AND 50;",
            f"Find rows where {random.choice(numerical)} is between 10 and 50 in the {table} table."
        ),
        (
            f"SELECT {random.choice(numerical)} FROM {table} WHERE {random.choice(non_numerical)} = 'SampleValue';",
            f"Find rows where {random.choice(non_numerical)} equals 'SampleValue' in the {table} table."
        )
    ] if numerical else [],

    "join": lambda: [
        (
            f"SELECT c.Name, s.Order_Date, p.Product_Name "
            f"FROM Customers c JOIN Sales s ON c.CustomerKey = s.CustomerKey "
            f"JOIN Products p ON s.ProductKey = p.ProductKey;",
            "Join the Customers, Sales, and Products tables to retrieve customer names, order dates, and product names."
        ),
        (
            f"SELECT s.Order_Number, s.Quantity, p.Product_Name "
            f"FROM Sales s JOIN Products p ON s.ProductKey = p.ProductKey;",
            "Join the Sales and Products tables to retrieve order numbers, quantities, and product names."
        ),
        (
            f"SELECT c.Name, COUNT(*) FROM Customers c JOIN Sales s ON c.CustomerKey = s.CustomerKey GROUP BY c.Name;",
            "Group by customer names to count the number of sales per customer by joining Customers and Sales tables."
        )
    ],

    "max": lambda: [
        (
            f"SELECT MAX({attr}) AS Max_{attr} FROM {table};",
            f"Find the maximum value of {attr} in the {table} table."
        ) for attr in numerical
    ] + [
        (
            f"SELECT {non_agg}, MAX({attr}) FROM {table} GROUP BY {non_agg};",
            f"Group by {non_agg} and find the maximum value of {attr} in the {table} table."
        ) for non_agg in non_numerical for attr in numerical
    ] if numerical and non_numerical else [],

    "avg": lambda: [
        (
            f"SELECT AVG({attr}) AS Avg_{attr} FROM {table};",
            f"Find the average value of {attr} in the {table} table."
        ) for attr in numerical
    ] + [
        (
            f"SELECT {non_agg}, AVG({attr}) FROM {table} GROUP BY {non_agg};",
            f"Group by {non_agg} and find the average value of {attr} in the {table} table."
        ) for non_agg in non_numerical for attr in numerical
    ] if numerical and non_numerical else [],

    "min": lambda: [
        (
            f"SELECT MIN({attr}) AS Min_{attr} FROM {table};",
            f"Find the minimum value of {attr} in the {table} table."
        ) for attr in numerical
    ] + [
        (
            f"SELECT {non_agg}, MIN({attr}) FROM {table} GROUP BY {non_agg};",
            f"Group by {non_agg} and find the minimum value of {attr} in the {table} table."
        ) for non_agg in non_numerical for attr in numerical
    ] if numerical and non_numerical else [],

    "count": lambda: [
        (
            f"SELECT COUNT(*) FROM {table};",
            f"Count the total number of rows in the {table} table."
        )
    ] + [
        (
            f"SELECT {non_agg}, COUNT(*) FROM {table} GROUP BY {non_agg};",
            f"Group by {non_agg} and count occurrences in the {table} table."
        ) for non_agg in non_numerical
    ] if non_numerical else [],

    "order by": lambda: [
        (
            f"SELECT {random.choice(numerical)} FROM {table} ORDER BY {random.choice(numerical)} DESC LIMIT 5;",
            f"Retrieve the top 5 values of {random.choice(numerical)} from the {table} table in descending order."
        ),
        (
            f"SELECT {random.choice(numerical)} FROM {table} ORDER BY {random.choice(numerical)} ASC LIMIT 5;",
            f"Retrieve the top 5 values of {random.choice(numerical)} from the {table} table in ascending order."
        ),
        (
            f"SELECT {random.choice(numerical + non_numerical)} FROM {table} ORDER BY {random.choice(numerical)} LIMIT 10;",
            f"Retrieve the top 10 rows of {random.choice(numerical + non_numerical)} ordered by {random.choice(numerical)} in the {table} table."
        )
    ] if numerical else [],

    "sum": lambda: [
        (
            f"SELECT SUM({attr}) AS Total_{attr} FROM {table};",
            f"Calculate the total sum of {attr} in the {table} table."
        ) for attr in numerical
    ] + [
        (
            f"SELECT {non_agg}, SUM({attr}) FROM {table} GROUP BY {non_agg};",
            f"Group by {non_agg} and calculate the total sum of {attr} in the {table} table."
        ) for non_agg in non_numerical for attr in numerical
    ] if numerical and non_numerical else [],

    "distinct": lambda: [
        (
            f"SELECT DISTINCT {attr} FROM {table};",
            f"Find all distinct values of {attr} in the {table} table."
        ) for attr in non_numerical
    ] + [
        (
            f"SELECT {non_agg}, COUNT(DISTINCT {random.choice(numerical)}) FROM {table} GROUP BY {non_agg};",
            f"Group by {non_agg} and count distinct values of {random.choice(numerical)} in the {table} table."
        ) for non_agg in non_numerical
    ] if non_numerical else [],
}



    # Generate queries for the given command
    return command_patterns.get(command, lambda: [])()



@app.route('/natural_language_query', methods=['GET', 'POST'])
def natural_language_query():
    database = "chatdb"  

    if request.method == 'GET':
        return """
        <h1>Natural Language Query</h1>
        <form action="/natural_language_query" method="post">
            <label for="nl_query">Enter your question in natural language:</label><br>
            <textarea id="nl_query" name="nl_query" rows="4" cols="50" placeholder="E.g., find total sales amount broken down by product category"></textarea><br><br>
            <button type="submit">Submit</button>
        </form>
        <p><a href="/">Back to Home</a></p>
        """
    elif request.method == 'POST':
        nl_query = request.form.get('nl_query', '').lower()

        if not nl_query:
            return "<h3>Error: No question provided.</h3><p><a href='/natural_language_query'>Try Again</a></p>", 400

        # Match query pattern
        query, description = match_nl_query(nl_query)

        if query:
            return f"""
            <h1>Natural Language Query Result</h1>
            <h4>Question:</h4>
            <pre>{nl_query}</pre>
            <h4>Description:</h4>
            <p>{description}</p>
            <h4>Generated SQL Query:</h4>
            <pre>{query}</pre>
            <form action="/execute_query" method="post">
                <input type="hidden" name="database" value="{database}">
                <input type="hidden" name="query" value="{query}">
                <button type="submit">Execute Query</button>
            </form>
            <p><a href="/natural_language_query">Ask Another Question</a></p>
            <p><a href="/">Back to Home</a></p>
            """
        else:
            return f"<h3>No matching query pattern found for: {nl_query}</h3><p><a href='/natural_language_query'>Try Again</a></p>"




def match_nl_query(nl_query, database="chatdb"):
    """Matches natural language queries to SQL queries based on recognized patterns."""

    words = nl_query.lower().split()

    table_schemas = {  
        "Sales": ["Order_Number", "Line_Item", "Order_Date", "Delivery_Date", "CustomerKey", "StoreKey", "ProductKey", "Quantity", "Currency_Code"],
        "Products": ["ProductKey", "Product_Name", "Brand", "Color", "Unit_Cost_USD", "Unit_Price_USD", "Category", "Subcategory"],
        "Customers": ["CustomerKey", "Name", "City", "State_Code", "State", "Zip_Code", "Country", "Continent", "Birthday"],
    }
    all_attributes = {}
    for table, attrs in table_schemas.items():
        for attr in attrs:
            all_attributes[attr.lower()] = (table, attr)  
            all_attributes[attr.lower().replace("_", "")] = (table, attr) 


    recognized_attributes = []
    for word in words:
        if word in all_attributes:
          recognized_attributes.append(all_attributes[word])
    if not recognized_attributes:
        return None, "No attributes recognized."


    def generate_joins(tables_needed):
        joins = []
        if "Sales" in tables_needed and "Products" in tables_needed:
            joins.append("JOIN Products p ON s.ProductKey = p.ProductKey")
        if "Sales" in tables_needed and "Customers" in tables_needed:
            joins.append("JOIN Customers c ON s.CustomerKey = c.CustomerKey")
        return " " + " ".join(joins) if joins else ""


    def aggregate_pattern(aggregate_func, quantity_attr=None):
        tables_needed = {"Sales"}
        group_by_attr = None
        group_by_table = "Sales" 


        try:
            by_index = words.index("by")
            group_by_attr_word = words[by_index + 1]
            group_by_table, group_by_attr = all_attributes.get(group_by_attr_word, ("Sales", None)) # Use get with default
            tables_needed.add(group_by_table)
        except (ValueError, IndexError):
            pass

        if quantity_attr == "*": 
            select_clause = f"{aggregate_func}({quantity_attr})"
        elif quantity_attr:
            select_attr_table, select_attr = all_attributes.get(quantity_attr, (None,None))
            if select_attr_table:
                tables_needed.add(select_attr_table)
                select_clause = f"{aggregate_func}({select_attr_table.lower()[0]}.{select_attr})"
            else:
                select_clause = f"{aggregate_func}(s.Quantity)" 
                tables_needed.add("Sales") 
        else:
            select_clause = f"{aggregate_func}(s.Quantity)"

        if "revenue" in words or "cost" in words or "price" in words:
            select_clause = "SUM(p.Unit_" + ("Price_USD" if "revenue" in words or "price" in words else "Cost_USD") + "* s.Quantity)"
            tables_needed.update({"Products", "Customers"}) 


        joins = generate_joins(tables_needed)


        query = f"SELECT {group_by_table.lower()[0]}.{group_by_attr or '*'}, {select_clause} AS AggregatedValue FROM Sales s {joins}"
        if group_by_attr:  
            query = f"SELECT {group_by_table.lower()[0]}.{group_by_attr}, {select_clause} AS AggregatedValue FROM Sales s {joins}"
        else:  
            query = f"SELECT {select_clause} AS AggregatedValue FROM Sales s {joins}"

        if group_by_attr:
            query += f" GROUP BY {group_by_table.lower()[0]}.{group_by_attr}"
        query += ";"

        return query, f"{aggregate_func.capitalize()} of {quantity_attr or 'Quantity'} grouped by {group_by_attr or 'All'}"




    if "total" in words:
        return aggregate_pattern("SUM")

    elif "average" in words:
        return aggregate_pattern("AVG")


    elif "count" in words:
      try:
          of_index = words.index("of") if "of" in words else words.index("number")
          count_attr = words[of_index + 1]
      except (ValueError, IndexError):
          count_attr = "*"

      return aggregate_pattern("COUNT", count_attr)

    elif "distinct" in words:
        try:
            distinct_index = words.index("distinct")
            distinct_attr = words[distinct_index+1]
            distinct_table, distinct_attribute = all_attributes[distinct_attr]
            return f"SELECT DISTINCT {distinct_attribute} FROM {distinct_table};", f"Distinct values of {distinct_attr}"
        except (ValueError, IndexError, KeyError):
            return None, "Invalid distinct query."


    elif words[0] == "top" or words[0] == "lowest": 
        try:
            num = int(words[1])
            asc_desc = "DESC" if words[0] == "top" else "ASC"  

            order_by_index = words.index("by") if "by" in words else -1  
            if order_by_index != -1:
                order_by_attr_word = words[order_by_index + 1]
                order_by_table, order_by_attr = all_attributes.get(order_by_attr_word, (None, None))

            else:
                order_by_table, order_by_attr = None, None  

            if 2 < len(words):  
                table, attribute = all_attributes.get(words[2], (None, None))
                if not table or not attribute:
                    table, attribute = all_attributes.get(words[3], (None, None))

            else: 
                table, attribute = "Sales", "Quantity"

            tables_needed = {table or "Sales"}  
            if order_by_table:
                tables_needed.add(order_by_table)
            aggregate = False  


            if attribute:  
                if attribute.lower() in ["cost", "price", "revenue"]:
                    tables_needed.add("Products")
                    table = "Products"
                    attribute_original = attribute  
                    attribute = "Unit_" + (attribute.title() + "_USD")  
                    aggregate = True 

            joins_str = generate_joins(tables_needed)


            if order_by_attr:  
                if table and order_by_table: 
                    if aggregate: 
                        query = f"SELECT {order_by_table.lower()[0]}.{order_by_attr}, SUM({table.lower()[0]}.{attribute} * s.Quantity) AS Total FROM Sales s {joins_str} GROUP BY {order_by_table.lower()[0]}.{order_by_attr} ORDER BY Total {asc_desc} LIMIT {num};"
                        description = f"{words[0].capitalize()} {num} {attribute_original.lower()} by {order_by_attr}"

                    else: 
                      query = f"SELECT {order_by_table.lower()[0]}.{order_by_attr}, {table.lower()[0]}.{attribute} FROM Sales s {joins_str} ORDER BY {table.lower()[0]}.{attribute} {asc_desc} LIMIT {num};"
                      description = f"{words[0].capitalize()} {num} {attribute.lower()} ordered by {order_by_attr}"

                else:
                   return None, "Invalid top/lowest query: Missing attribute or 'by' clause."

            elif table and attribute: 
                    if aggregate:
                        query = f"SELECT SUM({table.lower()[0]}.{attribute} * s.Quantity) AS Total FROM Sales s {joins_str} ORDER BY Total {asc_desc} LIMIT {num};"
                        description = f"{words[0].capitalize()} {num} {attribute_original.lower()}"


                    else:
                        query = f"SELECT {table.lower()[0]}.{attribute} FROM Sales s {joins_str} ORDER BY {table.lower()[0]}.{attribute} {asc_desc} LIMIT {num};"
                        description = f"{words[0].capitalize()} {num} {attribute.lower()}"
            else:
                return None, "Invalid top/lowest query structure."

            return query, description


        except (ValueError, IndexError, KeyError) as e:
            return None, f"Invalid top/lowest query. Check your input: {e}"





if __name__ == '__main__':
    app.run(debug=True)
