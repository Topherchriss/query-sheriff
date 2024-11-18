from datetime import datetime
import re
import logging
from django.conf import settings # type: ignore
from django.db import connection # type: ignore
from django.db.models import fields # type: ignore
from django.apps import apps # type: ignore
import hashlib
from django.core.cache import cache # type: ignore
from query_sheriff.inspector.suggestions import OptimizationSuggestion # type: ignore


logger = logging.getLogger(__name__)

IRRELEVANT_FIELDS = {
    'oid', 'chunk_id', 'last_value', 'attrelid', 'inhrelid', 'indexrelid', 'classid', 
    'objoid', 'srrelid', 'ev_class', 'mapcfg', 'proname', 'stxoid', 'roleid', 
    'starelid', 'rolname', 'partrelid', 'conrelid', 'subid', 'contypid', 'amopfamily',
    'tgconstraint', 'reltablespace', 'stxrelid', 'stxname', 'pubname', 'local_id',
    'collname', 'prrelid', 'typname', 'foreign_server_catalog', 'sequence_catalog',
    'implementation_info_id', 'constraint_catalog', 'nspname', 'domain_catalog', 
    'view_catalog', 'user_defined_type_catalog', 'table_catalog', 'function_id',
    'dbid', 'wal_records', 'pid', 'session_key', 'expire_date', 'feature_id', 'line_number'
}


# New logging levels
TIP_LEVEL = 25
SUGGESTION_LEVEL = 15

# Add the levels to the logging module
logging.addLevelName(TIP_LEVEL, 'TIP')
logging.addLevelName(SUGGESTION_LEVEL, 'SUGGESTION')

# logger method for TIP level
def tip(self, message, *args, **kwargs):
    if self.isEnabledFor(TIP_LEVEL):
        self._log(TIP_LEVEL, message, args, **kwargs)


# logger method for SUGGESTION level
def suggestion(self, message, *args, **kwargs):
    if self.isEnabledFor(SUGGESTION_LEVEL):
        self._log(SUGGESTION_LEVEL, message, args, **kwargs)

# Add the methods to the Logger class
logging.Logger.tip = tip
logging.Logger.suggestion = suggestion

def log_and_raise_error(message, exception=None):
    """
    Logs the error message and raises an exception if provided.
    """
    logger.error(message)
    if exception:
        raise exception

def sanitize_string(input_string):
    return input_string.replace('\0', '') if input_string else input_string # filter out null bytes from cached 

class QueryCache:
    """
    Caches the results of EXPLAIN queries using Redis.
    """
    def __init__(self, cache_timeout=3600):
        self.cache_timeout = cache_timeout  # timeout: 1 hour

    def get_query_hash(self, sql):
        """
        Generate a hash from the SQL query to use as the cache key.
        """
        return hashlib.sha256(sql.encode('utf-8')).hexdigest()

    def get_cached_explain(self, sql):
        """
        Retrieve cached EXPLAIN result for a query from Redis.
        """
        query_hash = self.get_query_hash(sql)
        result = cache.get(query_hash)
        return sanitize_string(result) if result else None

    def set_cached_explain(self, sql, explain_result):
        """
        Cache the EXPLAIN result for a query in Redis.
        """
        query_hash = self.get_query_hash(sql)
        cache.set(query_hash, explain_result, timeout=self.cache_timeout)


class ExplainQuery:
    """
    Handles EXPLAIN optimizations, using Redis caching.
    """
    def __init__(self, query_cache=None):
        self.query_cache = query_cache or QueryCache()

    def explain_query(self, sql, params=None):
        """
        Runs EXPLAIN on a query if it is not already cached.
        Returns the EXPLAIN result, checking Redis cache first.
        """
        # Check if EXPLAIN result is cached
        cached_result = self.query_cache.get_cached_explain(sql)
        if cached_result:
            return cached_result

        # If query contains placeholders like %s, simplify it; otherwise, assume it's raw SQL
        if '%s' in sql or re.search(r"\$\d+", sql):
            valid_sql = simplify_query(sql)
        else:
            valid_sql = sql

        # Run EXPLAIN and cache the result
        with connection.cursor() as cursor:
            cursor.execute(f"EXPLAIN {valid_sql}", params)
            query_plan = cursor.fetchall()

        # Cache the EXPLAIN result
        self.query_cache.set_cached_explain(sql, query_plan)
        return query_plan


class WriteInefficienciesToFile:
    def log_inefficiencies_to_file(self, inefficiencies, file_path):
        """
        Logs detected inefficiencies to a specified log file.
        """
        suggestion_handler = OptimizationSuggestion()
        logged_inefficiencies = set()

        with open(file_path, 'a') as log_file:  # Open the log file in append mode
            for inefficiency in inefficiencies:
                inefficiency_type = inefficiency['type']
                query = inefficiency['query']
                count = inefficiency.get('count', 1)
                suggestion = inefficiency.get('suggestion', 'No suggestion available')

                if inefficiency_type in logged_inefficiencies:
                    continue  # Skip already logged inefficiency

                # Extract and log inefficiency details
                source = inefficiency.get('source', 'Unknown')

                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_file.write(f"\n--- Inefficiency Detected at {current_time} ---\n")
                log_file.write(f"Type: {inefficiency_type}\n")
                log_file.write(f"Occurrences: {count}\n")
                log_file.write(f"Problematic Query:\n{query[:500]} ... [truncated]\n")
                log_file.write(f"Source: {source}\n")
                log_file.write("--------------------------------\n")
                
                # Generate and log suggestion
                generated_suggestion = suggestion_handler.suggest({
                    'type': inefficiency_type,
                    'query': query,
                    'suggestion': suggestion
                })
                
                log_file.write("\n--- Optimization Tip ---\n")
                log_file.write(f" {generated_suggestion}\n")
                log_file.write("--------------------------------\n")

                # Mark this inefficiency as logged
                logged_inefficiencies.add(inefficiency_type)


### N Plus one ###

def simplify_query(sql):
    """
    Simplifies the SQL query by removing dynamic parts like WHERE conditions, ORDER BY,
    and placeholders for normalization. Helps in detecting repeated queries.
    """
    try:
        # Normalize parameter placeholders (PostgreSQL, MySQL, SQLite, etc.)
        normalized_sql = re.sub(r"\$\d+", "?", sql)  # PostgreSQL placeholders ($1, $2)
        normalized_sql = re.sub(r"%s", "?", normalized_sql)  # MySQL placeholders (%s)
        normalized_sql = re.sub(r"\?", "?", normalized_sql)  # General placeholders

        # Normalize and remove clauses that change frequently
        normalized_sql = re.sub(r"WHERE\s+.*?(GROUP BY|ORDER BY|$)", "", normalized_sql, flags=re.IGNORECASE)
        normalized_sql = re.sub(r"ORDER\s+BY\s+((?:\"?[\w_]+\"?(?:\.\"?[\w_]+\"?)?(?:\s+ASC|\s+DESC)?)(?:\s*,\s*\"?[\w_]+\"?(?:\.\"?[\w_]+\"?)?(?:\s+ASC|\s+DESC)?)*)(?:\s*|\s*;)", "", normalized_sql, flags=re.IGNORECASE)
        normalized_sql = re.sub(r"GROUP BY\s+.*?(ORDER BY|$)", "", normalized_sql, flags=re.IGNORECASE)

        # Trim and normalize any remaining whitespace
        normalized_sql = re.sub(r"\s+", " ", normalized_sql).strip()

        return normalized_sql

    except Exception as e:
        # Log error and return original SQL if normalization fails
        logger.error(f"Error simplifying query: {sql}. Exception: {e}")
        return sql


def detect_repeated_queries_for_related_objects(seen_queries):
    """
    Analyzes the seen queries and checks if related objects are being queried repeatedly.
    Suggests the use of select_related or prefetch_related based on the query pattern.
    """
    related_object_suggestions = []

    for _ , details in seen_queries.items():
        count = details['count']
        raw_sql = details['raw_sql']

        # If the same simplified query is repeated multiple times, flag it as potential N+1
        if count > 1:
            # Heuristic to check if it's a query on related models (ForeignKey, OneToOne)
            if "JOIN" not in raw_sql and "SELECT" in raw_sql and "WHERE" in raw_sql:
                # Recommend: select_related or prefetch_related
                if "LIMIT" in raw_sql:
                    suggestion = "Consider using select_related to optimize this query."
                else:
                    suggestion = "Consider using prefetch_related to optimize this query."

                related_object_suggestions.append({
                    'type': 'N+1 Query',
                    'query': raw_sql,
                    'count': count,
                    'suggestion': suggestion
                })
    
    return related_object_suggestions


def ignore_legitimate_batches_and_transactions(queries):
    """
    Filter out legitimate batch operations and transactions to avoid false positives.
    """
    filtered_queries = []

    for query in queries:
        if not isinstance(query, dict):
            continue

        sql = query['sql']

        # Ignore transaction-related queries
        if re.search(r"\b(BEGIN|COMMIT|ROLLBACK)\b", sql, re.IGNORECASE):
            continue

        # Ignore bulk inserts or updates (assuming multi-row inserts)
        if re.search(r"\b(INSERT INTO|UPDATE)\b.*?\bVALUES\b", sql, re.IGNORECASE):
            continue

        filtered_queries.append(query)

    return filtered_queries


## Generic method for missing indexes ###

def detect_missing_indexes_for_clause(query, tables, columns, clause_type):
    """
    Generic function to detect missing indexes for a specific clause (WHERE, JOIN, ORDER BY, AGGREGATE).
    """
    missing_index_queries = []

    for table in tables:
        if is_small_table(table):
            continue  # Skip small tables that won't benefit from indexing.

        # Check for composite index (if more than a column is used)
        if len(columns) > 1:
            if not is_composite_indexed(table, columns):
                index_suggestion = generate_index_suggestion(table, columns)
                missing_index_queries.append({
                    'type': f'Missing Index on {clause_type}',
                    'query': query['sql'],
                    'table': table,
                    'columns': columns,
                    'suggestion': f'Columns {columns} involved in {clause_type} lack a composite index.\n{index_suggestion}'
                })
        else:
            # Single-column index check
            for column in columns:
                if not is_column_indexed(table, [column]):
                    if not is_indexed(query):  # Run EXPLAIN if no index is found
                        index_suggestion = generate_index_suggestion(table, [column])
                        missing_index_queries.append({
                            'type': f'Missing Index on {clause_type}',
                            'query': query['sql'],
                            'table': table,
                            'columns': [column],
                            'suggestion': f'Column {column} involved in {clause_type} lacks an index.\n{index_suggestion}'
                        })

    return missing_index_queries


### Distinct detection ###

def extract_select_clause(sql):
    """
    Extracts the SELECT clause from the SQL query, handling nested queries.
    The SELECT clause is assumed to be everything between 'SELECT' and the main 'FROM'.
    """
    try:
        # Regex matching the SELECT clause while ignoring subqueries
        match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        # match = re.search(r'SELECT\s+((?:[^()]+|\([^()]*\))*)\s+FROM', sql, re.IGNORECASE | re.DOTALL)

        if match:
            select_clause = match.group(1).strip()
            # Remove any nested queries within the SELECT clause
            select_clause = re.sub(r'\(.*?\)', '', select_clause)
            return select_clause
    except Exception as e:
        log_and_raise_error("An error occured extracting select clause", str(e))

    return None


def extract_table_names(sql):
    """
    Extracts table names from SQL queries, handling joins and multiple tables.
    """
    try:
        # Handle queries with JOINs as well. Capture multiple table names.
        matches = re.findall(r'FROM\s+([\"\'\w\.]+)(?:\s+AS\s+\w+)?|JOIN\s+([\"\'\w\.]+)', sql, re.IGNORECASE)

        # Flatten the match tuples and filter out None values
        table_names = {match[0] or match[1] for match in matches if match}
    except Exception as e:
        log_and_raise_error("An error occured extracting table names", str(e))

    return list(table_names)


def get_primary_keys(table_name):
    """
    Retrieves the primary keys for the given table in a database-agnostic way.
    Using database schema reflection through Django's connection.
    """
    with connection.cursor() as cursor:
        try:
            # based on the database backend
            if connection.vendor == 'sqlite':
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                primary_keys = [column[1] for column in columns if column[5] == 1]
            elif connection.vendor == 'postgresql':
                cursor.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND i.indisprimary;
                """, [table_name])
                primary_keys = [row[0] for row in cursor.fetchall()]
            elif connection.vendor == 'mysql':
                cursor.execute(f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = %s AND COLUMN_KEY = 'PRI';
                """, [table_name])
                primary_keys = [row[0] for row in cursor.fetchall()]
            else:
                primary_keys = []
        except Exception as e:
            log_and_raise_error("An error occured getting primary keys", str(e))

    return primary_keys


def get_unique_fields(table_name):
    """
    Retrieve the unique fields for a given table, in a database-agnostic way.
    This method checks for unique constraints or unique indexes.
    """
    with connection.cursor() as cursor:
        try:

            if connection.vendor == 'sqlite':
                cursor.execute(f"PRAGMA index_list({table_name})")
                indexes = cursor.fetchall()
                unique_fields = set()
                for index in indexes:
                    if index[2]:  # Check if the index is unique
                        cursor.execute(f"PRAGMA index_info({index[1]})")
                        unique_fields.update([info[2] for info in cursor.fetchall()])
            elif connection.vendor == 'postgresql':
                cursor.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attnum = ANY(i.indkey)
                    JOIN pg_class c ON c.oid = i.indexrelid
                    WHERE i.indrelid = %s::regclass AND i.indisunique;
                """, [table_name])
                unique_fields = {row[0] for row in cursor.fetchall()}
            elif connection.vendor == 'mysql':
                cursor.execute(f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = %s AND NON_UNIQUE = 0;
                """, [table_name])
                unique_fields = {row[0] for row in cursor.fetchall()}
            else:
                unique_fields = set()
        except Exception as e:
            log_and_raise_error("An error occured getting unique fields", str(e))

    return unique_fields


def normalize_column_name(column_name):
    # Normalize the column names by striping the prefix 
    normalized_name = column_name.split('.')[-1].replace('"', '').lower().strip()

    # Remove the "on" if it's part of the name
    if 'on' in normalized_name:
        normalized_name = normalized_name.replace('on', '').strip()
    return normalized_name


def filter_primary_keys(primary_keys):
    """Filter out irrelevant primary keys."""
    # Keep only meaningful primary keys, excluding irrelevant fields
    return {key for key in primary_keys if key not in IRRELEVANT_FIELDS}


def is_distinct_unnecessary(select_clause, table_names):
    """
    Determines if DISTINCT is unnecessary by analyzing the query.
    - If the DISTINCT is on a primary key or unique field from any of the tables, it's unnecessary.
    - If the query inherently prevents duplicates, DISTINCT is unnecessary.
    """
    if not select_clause or not table_names:
        return False  # Can't procced if extracting the SELECT clause or tables fails

    # Initialize a set for storing unique fields across all tables
    all_unique_fields = set()

    # Iterate over each table involved
    for table_name in table_names:
        primary_keys = get_primary_keys(table_name)
        unique_fields = get_unique_fields(table_name)

        # Filter out irrelevant primary keys and normalize
        filtered_primary_keys = filter_primary_keys(primary_keys)

        # Normalize and add the primary keys and unique fields to the combined set
        all_unique_fields.update(normalize_column_name(pk) for pk in filtered_primary_keys)
        all_unique_fields.update(normalize_column_name(uf) for uf in unique_fields)

    # Check if the DISTINCT ON clause is present
    distinct_on_match = re.search(r'\bDISTINCT\s+ON\s*\(\s*([^\)]+)\s*\)', select_clause, re.IGNORECASE)
    distinct_column = None

    if distinct_on_match:
        # Handle DISTINCT ON case
        distinct_column = distinct_on_match.group(1).strip()
        distinct_column_normalized = normalize_column_name(distinct_column)

        if distinct_column_normalized in all_unique_fields:
            # Remove the DISTINCT ON part from the select clause for further analysis
            select_clause = select_clause.replace(distinct_on_match.group(0), '').strip(',')
    else:
        # Handle standard DISTINCT case
        distinct_match = re.search(r'\bDISTINCT\b', select_clause, re.IGNORECASE)
        if distinct_match:
            select_clause = select_clause.replace(distinct_match.group(0), '').strip()

    # Process the remaining selected columns
    selected_columns = [normalize_column_name(col) for col in select_clause.split(',') if col]

    # Check if all selected columns are unique (either primary keys or uniquely constrained)
    if all(col in all_unique_fields for col in selected_columns):
        return True

    return False


### Missing Indexed detection 

def get_model_from_table(table_name):
    """
    Retrieve the Django model class for the given table name.
    """
    for model in apps.get_models():
        if  model._meta.db_table == table_name:
            return model
    return None


def is_column_indexed(table_name, columns):
        """
        Check if the columns in the WHERE clause are indexed.
        """
        model = get_model_from_table(table_name)
        if model:
            for field in model._meta.fields:
                if field.name in columns and isinstance(field, fields.Field) and field.db_index:
                    return True
        return False


def is_indexed(query):
    """
    Run EXPLAIN on the query and check if it's using an index or doing a sequential scan.
    """
    query_plan_instance = ExplainQuery()
    query_plan = query_plan_instance.explain_query(query['sql'])
    if query_plan:
        # Parse the query plan for sequential scans
        for plan in query_plan:
            # Detect sequential scans (in PostgreSQL)
            if 'Seq Scan' in plan[0]:
                return False  # An index is missing
            if 'Index Scan' in plan[0]:
                return True  # No need to add an index
    return False 


def extract_table_and_column_names(sql):
    """
    Extract both table names and column names from SQL queries, handling WHERE clauses and JOINs.
    """
    # Using extract_table_names function to get table names
    table_names = extract_table_names(sql)

    # Extract column names from WHERE clauses
    try:
        # Capture column names mentioned in the WHERE clause
        # Looking for patterns like WHERE column = value, AND column = value ...
        column_matches = re.findall(r'WHERE\s+([\"\'\w\.]+)\s*=|AND\s+([\"\'\w\.]+)\s*=', sql, re.IGNORECASE)

        # Flatten the match tuples and filter out None values
        columns = {match[0] or match[1] for match in column_matches if match}
    except Exception as e:
        log_and_raise_error("An error occurred extracting column names", str(e))

    return list(table_names), list(columns)


def is_small_table(table_name):
        """
        Determine if the table has too few rows to justify adding an index.
        """
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = '{table_name}'")
            result = cursor.fetchone()
            threshold = getattr(settings, 'SMALL_TABLE_THRESHOLD', 100)
            return result and result[0] < threshold  # Ignore tables with less than 100 rows


def is_composite_indexed(table_name, columns):
    """
    Check if a composite index exists on the given table and columns.
    """
    try:
        with connection.cursor() as cursor:
            # Get the constraints and indexes on the table
            constraints = connection.introspection.get_constraints(cursor, table_name)

        # Loop through the constraints to check for an existing composite index
        for _ , constraint_info in constraints.items(): 
            # Check if it's an index and if it includes all the columns (in order)
            if constraint_info['index'] and set(columns) == set(constraint_info['columns']):
                return True  # Composite index exists

        return False  # No composite index found
    except Exception as e:
        logger.error('An error occured: %s', str(e))


def generate_index_suggestion(table_name, columns):
    # Generate a SQL index creation suggestion, considering composite indexes for multiple columns.

    # Handle cases where table_name is a list
    if isinstance(table_name, list):
        # The first table in the list is the target for indexing.
        table_name = table_name[0]

    # Clean the table name by removing quotes and trimming spaces
    clean_table_name = table_name.replace('"', '').strip()

    # Apply regex to clean and parse columns with aliases
    def clean_column_name(column):
        # Remove any table aliases and strip quotes
        return re.sub(r'[A-Za-z0-9_]+\.', '', column).replace('"', '').strip()

    # Ensure columns are a list of valid strings
    if isinstance(columns, str):
        # Split columns string into a list.
        columns = columns.split(',')
    
    # Clean the columns
    clean_columns = [clean_column_name(col) for col in columns]

    # Ensure valid column names, not individual characters
    if any(len(col) <= 1 for col in clean_columns):  # Catch invalid columns
        raise ValueError("Column names appear to be split into individual characters.")

    try:
        # Generate the index name based on multiple columns when necessary
        if len(clean_columns) > 1:
            index_name = f"idx_{clean_table_name}_{'_'.join(clean_columns)}"
            # Create composite index suggestion
            index_sql = f"CREATE INDEX {index_name} ON {clean_table_name}({', '.join(clean_columns)});"
        else:
            index_name = f"idx_{clean_table_name}_{clean_columns[0]}"
            # Create single column index suggestion
            index_sql = f"CREATE INDEX {index_name} ON {clean_table_name}({clean_columns[0]});"
    except Exception as e:
        logger.error('An error occured generating an index: %s', str(e))
    
    return index_sql


## Missing indexes in JOINs ##

def extract_joins_from_query(sql):
    """
    Extract JOIN clauses from the SQL query, handling variations in JOIN syntax.
    """
    try:
        # Regex pattern matching different types of JOINs, including optional spacing and quotes around table names
        join_pattern = re.compile(
            r'\b(?:INNER|LEFT|RIGHT|FULL|OUTER)?\s*JOIN\s+"?([a-zA-Z0-9_"]+)"?\s+ON\s+\("?([a-zA-Z0-9_".]+)"?\s*=\s*"?([a-zA-Z0-9_".]+)"?\)',
               re.IGNORECASE
            )

        # join_pattern = re.compile(
            # r'\b(?:INNER|LEFT|RIGHT|FULL|OUTER)?\s*JOIN\s+("?[\w]+"?)\s+ON\s+("?[\w.]+")\s*=\s*("?[\w.]+")',
            # re.IGNORECASE)

        # Perform the search using the regex pattern
        matches = join_pattern.findall(sql)

        if matches:
            return matches
        else:
            return None
    except Exception as e:
        logger.error("An error occurred while extracting JOIN clauses: %s", str(e))
        return None


def get_join_columns(join_clause):
    """
    Extract the foreign key and referenced columns from a JOIN clause, supporting multiple columns.
    """
    try:
        # Clean the first element (joined table) to remove quotes
        joined_table = join_clause[0].replace('"', '').strip()

        # Clean the foreign key and referenced columns (handling multiple columns)
        fk_columns = join_clause[1].replace('"', '').strip()  # Foreign key columns
        ref_columns = join_clause[2].replace('"', '').strip()  # Referenced columns

        # Split the columns by comma to handle multiple columns
        fk_columns_list = [col.strip() for col in fk_columns.split(',')]
        ref_columns_list = [col.strip() for col in ref_columns.split(',')]

        # Ensure that columns are valid and there are equal numbers of foreign and referenced columns
        if not fk_columns_list or not ref_columns_list:
            raise ValueError("Foreign key or referenced columns cannot be empty.")
        
        if len(fk_columns_list) != len(ref_columns_list):
            raise ValueError("Mismatched number of foreign key and referenced columns.")

        return fk_columns_list, ref_columns_list
    except Exception as e:
        logger.error("An error occurred in get_join_columns: %s", {str(e)})
        raise e


### Inefficient ORDER_BY  ###

def extract_order_by_columns(sql):
    """
    Extract columns used in the ORDER BY clause from the SQL query.
    Handles basic cases where columns are directly referenced.

    Returns a list of columns or an empty list if no ORDER BY clause exists.
    """
    try:
        # Check for 'ORDER BY' without columns to handle invalid cases
        if re.search(r"ORDER\s+BY\s*(;|$)", sql, re.IGNORECASE):
            raise ValueError("ORDER BY clause contains empty or invalid columns.")
        
        # Regex pattern to match the ORDER BY clause and columns
        order_by_pattern = r"ORDER\s+BY\s+((?:\"[\w_]+\"\.\"[\w_]+\"(?:\s+ASC|\s+DESC)?)(?:\s*,\s*\"[\w_]+\"\.\"[\w_]+\"(?:\s+ASC|\s+DESC)?)*)(?:\s*|\s*;)"
        
        # Search for the ORDER BY clause in the query
        match = re.search(order_by_pattern, sql, re.IGNORECASE)

        if match:
            # Extract the matched string
            order_by_columns_str = match.group(1)

            # Split the columns by commas and remove extra spaces
            order_by_columns = [col.strip() for col in order_by_columns_str.split(',')]
            
            # Remove any ASC or DESC from the column names
            cleaned_order_by_columns = [re.sub(r'\s+(ASC|DESC)', '', col, flags=re.IGNORECASE).strip() for col in order_by_columns]

            # Ensure the columns are valid and not empty
            if any(not col for col in cleaned_order_by_columns):
                raise ValueError("ORDER BY clause contains empty or invalid columns.")
            
            return cleaned_order_by_columns
        else:
            # No ORDER BY clause found
            return []
    
    except Exception as e:
        # Log the error and re-raise it
        logger.error(f"An error occurred while extracting ORDER BY columns: {str(e)}")
        raise e


### Inefficient Aggregate functions ###

def extract_aggregate_functions(sql):
    try:
        # Regex pattern to match aggregate functions including nested functions inside the aggregate
        aggregate_pattern = r"\b(SUM|AVG|COUNT|MIN|MAX)\s*\(\s*([^)]+)\s*\)"
        
        # Find all matches of aggregate functions in the query
        aggregates = re.findall(aggregate_pattern, sql, re.IGNORECASE)
        
        # If any matches are empty
        if any(col.strip() == "" for _, col in aggregates):
            raise ValueError("Aggregate function contains empty or invalid columns.")
        
        aggregate_columns = [] # Store the cleaned column names

        for _, col in aggregates:
            cleaned_col = re.sub(r'\b\w+\s*\(\s*', '', col) # Replace nested functions with an empty string

            cleaned_col = re.sub(r'\)', '', cleaned_col) # Remove closing parentheses for the nested function
            
            cleaned_col = cleaned_col.replace('"', '').strip() # Remove quotes around column names

            aggregate_columns.append(cleaned_col)
        
        # Ensure columns are valid
        if any(not col for col in aggregate_columns):
            raise ValueError("Aggregate function contains empty or invalid columns.")
        
        return aggregate_columns if aggregate_columns else []
    
    except Exception as e:
        logger.error("An error occurred while extracting aggregate functions: %s", str(e))
        raise e