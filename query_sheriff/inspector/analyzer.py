import logging
import os
import re
from django.db import connection # type: ignore
from django.test import Client # type: ignore
from django.conf import settings # type: ignore
from .utils import (
    detect_missing_indexes_for_clause, detect_repeated_queries_for_related_objects, extract_joins_from_query,
    extract_order_by_columns, get_join_columns,
    ignore_legitimate_batches_and_transactions,
    simplify_query, extract_select_clause, extract_table_names,
    is_distinct_unnecessary, extract_table_and_column_names, extract_aggregate_functions
)
from .suggestions import (
    suggest_alternative_pagination, suggest_alternative_to_cartesian, suggest_alternative_to_subquery,
    suggest_avoiding_locks, suggest_limit_clause, suggest_lock_optimization,
    suggest_optimization_for_duplicate_query, suggest_optimization_for_slow_query,
    suggest_removing_distinct, suggest_sargable_query, suggest_specific_columns,
    suggest_transaction_optimization, suggest_where_clause
)

logger = logging.getLogger(__name__)


class QueryAnalyzer:
    def __init__(self, queries):
        self.queries = queries

    def analyze(self):
        inefficiencies = []
        inefficiencies += self.detect_n_plus_one()
        inefficiencies += self.detect_missing_indexes()
        inefficiencies += self.detect_join_without_index()
        inefficiencies += self.detect_inefficient_order_by()
        inefficiencies += self.detect_inefficient_aggregates()
        inefficiencies += self.detect_unnecessary_distinct()
        inefficiencies += self.detect_innefficient_use_of_subqueries()
        inefficiencies += self.detect_cartesian_product_in_joins()
        inefficiencies += self.detect_inefficient_pagination()
        inefficiencies += self.detect_non_sargable_queries()
        inefficiencies += self.detect_locking_issues()
        inefficiencies += self.detect_overuse_of_transactions()
        inefficiencies += self.detect_slow_queries()
        inefficiencies += self.detect_duplicate_queries()
        inefficiencies += self.detect_full_table_scan()
        inefficiencies += self.detect_missing_limit()
        inefficiencies += self.detect_inefficient_select()
        
        return inefficiencies
    
    def detect_n_plus_one(self):
        """
        Detect potential N+1 query problems by checking for repeated SELECTs with similar WHERE clauses
        on related objects without JOINs.
        """
        n_plus_one_queries = []
        seen_queries = {}

        filtered_queries = ignore_legitimate_batches_and_transactions(self.queries)

        for query in filtered_queries:
            if not isinstance(query, dict):
                continue

            sql = query['sql']

            # Check for SELECT queries with WHERE clauses
            if 'SELECT' in sql and 'WHERE' in sql:
                try:
                    # Simplify the SQL by removing unique parts
                    simplified_sql = simplify_query(sql)

                    # If the simplified query has been seen before, increment the count
                    if simplified_sql in seen_queries:
                        seen_queries[simplified_sql]['count'] += 1
                    else:
                        seen_queries[simplified_sql] = {'count': 1, 'raw_sql': sql}
                except Exception as e:
                    logger.error(f"Error simplifying query: {sql}. Exception: {e}")
                    continue

        # Track repeated queries and suggest optimization
        n_plus_one_queries.extend(detect_repeated_queries_for_related_objects(seen_queries))
    
        return n_plus_one_queries

    def detect_missing_indexes(self):
        """
        Detect queries that could benefit from missing indexes on WHERE clauses.
        """
        missing_index_queries = []
        for query in self.queries:
            if not isinstance(query, dict):
                continue 

            if 'WHERE' in query['sql']:
                tables, columns = extract_table_and_column_names(query['sql'])
                missing_index_queries += detect_missing_indexes_for_clause(query, tables, columns, 'WHERE')
        return missing_index_queries

    def detect_join_without_index(self):
        """
        Detect if a JOIN operation in the query lacks an index on the foreign key column.
        """
        missing_index_on_join_queries = []
        for query in self.queries:
            if not isinstance(query, dict):
                continue

            joins = extract_joins_from_query(query['sql'])
            tables, _ = extract_table_and_column_names(query['sql'])
            
            if not joins or not tables:
                continue

            for join in joins:
                try:
                    foreign_key_columns, referenced_columns = get_join_columns(join)
                    if foreign_key_columns and referenced_columns:
                        # Call the generic function to check foreign key columns for missing indexes
                        missing_index_on_join_queries += detect_missing_indexes_for_clause(query, tables, foreign_key_columns, 'JOIN')
                except Exception as e:
                    logger.error(f"Error processing inefficiency on JOIN clause: {str(e)}")
                    continue 
        return missing_index_on_join_queries

    def detect_inefficient_order_by(self):
        """
        Detect inefficient ORDER BY clauses where columns involved are not indexed.
        """
        inefficient_order_by_queries = []
        for query in self.queries:
            if not isinstance(query, dict):
                continue
            tables = extract_table_names(query['sql'])
            order_by_columns = extract_order_by_columns(query['sql'])
            if order_by_columns and tables:
                # Call the generic function to check ORDER BY columns for missing indexes
                inefficient_order_by_queries += detect_missing_indexes_for_clause(query, tables, order_by_columns, 'ORDER BY')
        return inefficient_order_by_queries

    def detect_inefficient_aggregates(self):
        """
        Detect inefficient AGGREGATE clauses where columns involved are not indexed.
        """
        inefficient_aggregate_queries = []
        for query in self.queries:
            if not isinstance(query, dict):
                continue
            tables = extract_table_names(query['sql'])
            aggregate_columns = extract_aggregate_functions(query['sql'])
            if aggregate_columns and tables:
                # Call the generic function to check AGGREGATE columns for missing indexes
                inefficient_aggregate_queries += detect_missing_indexes_for_clause(query, tables, aggregate_columns, 'AGGREGATE')
        return inefficient_aggregate_queries
    
    def detect_unnecessary_distinct(self):
        """
        Detect unnecessary use of DISTINCT in SQL queries, accounting for joins, multiple tables,
        and database-agnostic behavior.
        """
        unnecessary_distinct_queries = []

        for query in self.queries:
            if not isinstance(query, dict):
                continue
            sql = query['sql']

            # Ensure DISTINCT clause is present in the query
            if 'DISTINCT' in sql.upper():
                # Extract the SELECT clause and table names
                select_clause = extract_select_clause(sql)
                table_names = extract_table_names(sql)

                # If SELECT clause and table names successfully extracted...
                if select_clause and table_names:
                    if is_distinct_unnecessary(select_clause, table_names):
                        suggestion = suggest_removing_distinct(select_clause)
                        unnecessary_distinct_queries.append({
                            'type': 'Unnecessary DISTINCT',
                            'query': sql,
                            'suggestion': suggestion
                        })

        return unnecessary_distinct_queries
    
    def detect_innefficient_use_of_subqueries(self):
        """
        Detect overuse of subqueries and suggest alternatives (JOINs or CTEs).
        """
        subquery_inefficiencies = []

        for query in self.queries:
            if not isinstance(query, dict):
                continue
            sql = query['sql']

            # Detect subqueries in SELECT, WHERE, or JOIN clauses
            if re.search(r'\(SELECT .* FROM', sql, re.IGNORECASE):
                suggestion = suggest_alternative_to_subquery(sql)

                subquery_inefficiencies.append({
                    'type': 'Overuse of Subqueries',
                    'query': sql,
                    'suggestion': suggestion
                })

        return subquery_inefficiencies

    def detect_cartesian_product_in_joins(self):
        """
        Detect Cartesian products in JOIN clauses and suggest alternatives.
        """
        cartesian_product_inefficiencies = []

        for query in self.queries:
            if not isinstance(query, dict):
                continue

            sql = query['sql']

            # JOINs without ON condition might indicate a cartesian product
            if re.search(r'JOIN\s+\w+\s*(?!ON|USING)', sql, re.IGNORECASE):
                suggestion = suggest_alternative_to_cartesian(sql)
                cartesian_product_inefficiencies.append({
                    'type': 'Cartesian Product in JOIN',
                    'query': sql,
                    'suggestion': suggestion
                })

            # Detect explicit CROSS JOIN
            if 'CROSS JOIN' in sql.upper():
                suggestion = suggest_alternative_to_cartesian(sql)
                cartesian_product_inefficiencies.append({
                    'type': 'Cartesian Product in CROSS JOIN',
                    'query': sql,
                    'suggestion': suggestion
                })

        return cartesian_product_inefficiencies

    def detect_slow_queries(self, threshold=None):
        """
        Detect slow queries that exceed the configured time threshold.
        """
        if threshold is None:
            threshold = getattr(settings, 'SLOW_QUERY_THRESHOLD', 0.5)

        slow_queries = []
        for query in self.queries:
            if not isinstance(query, dict):
                continue

            execution_time = float(query['time'])
            if execution_time > threshold:
                suggestion = suggest_optimization_for_slow_query(query['sql'])
                slow_queries.append({
                    'type': 'Slow Query',
                    'query': query['sql'], 
                    'time': execution_time,
                    'suggestion': suggestion
                })

        return slow_queries

    def detect_duplicate_queries(self):
        """
        Detect identical SQL queries that are executed multiple times.
        """
        query_count = {}
        duplicate_queries = []

        for query in self.queries:
            if not isinstance(query, dict):
                continue

            sql = query['sql']
            query_count[sql] = query_count.get(sql, 0) + 1
            if query_count[sql] > 1:
                suggestion = suggest_optimization_for_duplicate_query(sql, query_count[sql])
                duplicate_queries.append({
                    'type': 'Duplicate Query',
                    'query': sql,
                    'count': query_count[sql],
                    'suggestion': suggestion
                })

        return duplicate_queries

    def detect_missing_limit(self):
        """
        Detects queries that are missing a LIMIT clause and could potentially return large datasets.
        """
        missing_limit_queries = []
        for query in self.queries:

            if not isinstance(query, dict):
                continue

            sql = query['sql']
            if 'SELECT' in sql and 'LIMIT' not in sql:
                suggestion = suggest_limit_clause(sql)
                missing_limit_queries.append({
                    'type': 'Missing LIMIT', 
                    'query': sql,
                    'suggestion': suggestion
                })
        return missing_limit_queries

    def detect_full_table_scan(self):
        """
        Detects queries without a WHERE clause that could cause full table scans.
        """
        full_table_scan_queries = []
        for query in self.queries:

            if not isinstance(query, dict):
                continue

            sql = query['sql']
            if 'SELECT' in sql and 'WHERE' not in sql:
                suggestion = suggest_where_clause(sql)
                full_table_scan_queries.append({
                    'type': 'Full Table Scan', 
                    'query': sql,
                    'suggestion': suggestion
                })
        return full_table_scan_queries

    def detect_inefficient_select(self):
        """
        Detects inefficient SELECT clauses, such as using SELECT * when only specific columns are needed.
        """
        inefficient_select_queries = []
        for query in self.queries:
            if not isinstance(query, dict):
                continue
            sql = query['sql']
            if 'SELECT *' in sql:
                suggestion = suggest_specific_columns(sql)
                inefficient_select_queries.append({
                    'type': 'Inefficient SELECT *', 
                    'query': sql,
                    'suggestion': suggestion
                })
        return inefficient_select_queries
    
    def detect_inefficient_pagination(self):
        """
        Detect inefficient pagination using large OFFSET values.
        """
        pagination_inefficiencies = []
        offset_threshold = getattr(settings, 'OFFSET_THRESHOLD', 500)  # threshold for large OFFSETs

        for query in self.queries:
            if not isinstance(query, dict):
                continue
            sql = query['sql']

            # Detect queries using LIMIT with large OFFSET values
            if 'LIMIT' in sql and 'OFFSET' in sql:
                offset_match = re.search(r'OFFSET\s+(\d+)', sql, re.IGNORECASE)
                if offset_match:
                    offset_value = int(offset_match.group(1))
                    if offset_value > offset_threshold:
                        suggestion = suggest_alternative_pagination()
                        pagination_inefficiencies.append({
                            'type': 'Inefficient Pagination',
                            'query': sql,
                            'suggestion': suggestion
                        })

        return pagination_inefficiencies

    def detect_non_sargable_queries(self):
        """
        Detect non-sargable queries that prevent the use of indexes.
        """
        non_sargable_inefficiencies = []

        for query in self.queries:
            if not isinstance(query, dict):
                continue

            sql = query['sql']

            # Detect non-sargable queries from functions or expressions in WHERE clauses
            if re.search(r'WHERE\s+.*\b(FUNCTION|EXPRESSION)\(.*\)', sql, re.IGNORECASE):
                suggestion = suggest_sargable_query()
                non_sargable_inefficiencies.append({
                    'type': 'Non-Sargable Query',
                    'query': sql,
                    'suggestion': suggestion
                })

        return non_sargable_inefficiencies

    def detect_locking_issues(self):
        """
        Detect potential locking issues due to long-running queries or explicit LOCK statements.
        """
        locking_inefficiencies = []
        lock_threshold = getattr(settings, 'LOCK_THRESHOLD', 5.0)  # Time in seconds for a query to be considered long

        for query in self.queries:
            if not isinstance(query, dict):
                continue
            sql = query['sql']
            execution_time = float(query.get('time', 0))

            # Detect long-running queries
            if execution_time > lock_threshold:
                suggestion = suggest_avoiding_locks(execution_time)
                locking_inefficiencies.append({
                    'type': 'Locking Issue',
                    'query': sql,
                    'execution_time': execution_time,
                    'suggestion': suggestion
                })

            # Detect explicit use of LOCK statements
            if 'LOCK' in sql.upper():
                suggestion = suggest_lock_optimization()
                locking_inefficiencies.append({
                    'type': 'Explicit LOCK Statement',
                    'query': sql,
                    'suggestion': suggestion
                })

        return locking_inefficiencies

    def detect_overuse_of_transactions(self):
        """
        Detect overuse of transactions, particularly long-running transactions.
        """
        transaction_inefficiencies = []
        transaction_threshold = getattr(settings, 'TRANSACTION_THRESHOLD', 5.0)  # Time in seconds for long transactions

        for query in self.queries:
            if not isinstance(query, dict):
                continue
            sql = query['sql']
            execution_time = float(query['time'])

            # Detect long-running transactions (BEGIN...COMMIT blocks)
            if 'BEGIN' in sql.upper() and 'COMMIT' in sql.upper() and execution_time > transaction_threshold:
                suggestion = suggest_transaction_optimization(execution_time)
                transaction_inefficiencies.append({
                    'type': 'Overuse of Transactions',
                    'query': sql,
                    'execution_time': execution_time,
                    'suggestion': suggestion
                })

        return transaction_inefficiencies


class QueryFetcher:
    
    @staticmethod
    def fetch_from_log_file(log_file_path):
        """
        Fetch SQL queries from a log file. Each query should be logged with a prefix 'SQL:'.
        """
        queries = []

        # Ensure the file exists
        if not os.path.exists(log_file_path):
            logger.error("Log file not found: %s", log_file_path)
            raise FileNotFoundError("Log file not found: %s", log_file_path)

        try:
            with open(log_file_path, 'r') as file:
                for line in file:
                    # Expecting a log format of 'SQL: <query>'
                    # match = re.search(r'SQL: (.+)', line)
                    match = re.search(r'SQL:\s*(.+)', line)

                    if match:
                        sql_query = match.group(1).strip()

                        if not QueryFetcher.is_valid_sql(sql_query):
                            logger.warning("Invalid SQL query: %s Skipping...", sql_query)
                            continue 

                        # Wrap each query in a dictionary with additional metadata
                        query_dict = {
                            'sql': sql_query,
                            'params': (), 
                            'time': '0.01',
                            'stack_trace': []
                        }
                        queries.append(query_dict)
        except Exception as e:
            logger.error("Error reading log file: %s", str(e))
            raise e
        
        if not queries:
            logger.info("No valid SQL queries provided.")

        return queries

    @staticmethod
    def fetch_from_simulated_request(view_name):
        """
        Simulate a request to a view and fetch the queries executed by that view.
        Uses `connection.queries_log` to access the queries executed.
        """
        client = Client()
        try:
            # Clear any previous queries logged before
            if hasattr(connection, 'queries_log'):
                connection.queries_log.clear()
            
            # Replace underscores with dashes
            response = client.get(f'/{view_name}/'.replace('_', '-'))

            # Check if the response status is OK
            if response.status_code != 200:
                logger.error("Failed to fetch view : %s Status code: %s", view_name, response.status_code)
                raise ValueError("View %s not found or failed to respond properly.", view_name)

            # Access the queries logged
            queries = []
            if hasattr(connection, 'queries_log'):
                queries = [q['sql'] for q in connection.queries_log]

            # Validate and filter out invalid queries
            queries = [q for q in queries if QueryFetcher.is_valid_sql(q)]
        except Exception as e:
            logger.error("Error simulating request to view : %s : error  :%s", view_name, str(e))
            raise e

        return queries

    @staticmethod
    def fetch_from_manual_input(queries):
        """
        Fetch SQL queries provided manually from the command line.
        Returns a list of dictionaries with query metadata.
        """
        validated_queries = []
        
        for query in queries:
            # Strip leading/trailing whitespaces
            sql_query = query.strip()
            
            # Ensure non-empty query
            if not sql_query:
                logger.warning("Empty SQL query provided. Skipping...")
                continue
            
            # Validate if it's a valid SQL query
            if not QueryFetcher.is_valid_sql(sql_query):
                logger.warning("Invalid SQL query: %s Skipping...", sql_query)
                continue
            
            # Wrap each query in a dictionary with additional metadata
            query_dict = {
                'sql': sql_query,
                'params': (), 
                'time': '0.01',
                'stack_trace': []
            }
            validated_queries.append(query_dict)

        if not validated_queries:
            logger.info("No valid SQL queries provided.")

        return validated_queries

    @staticmethod
    def is_valid_sql(sql):
        """
        Check if a query is valid SQL. Considered valid if it starts
        with SELECT, INSERT, UPDATE, DELETE and similar keywords
        """
        sql = sql.strip().upper()
        return bool(re.match(r'^(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)\s', sql))

                
        

