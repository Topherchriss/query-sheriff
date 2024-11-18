import time
import logging
import traceback
from pathlib import Path
from django.db import connection # type: ignore
from django.conf import settings # type: ignore
from django.db import DatabaseError, OperationalError # type: ignore
from .analyzer import QueryAnalyzer
from .suggestions import OptimizationSuggestion


logger = logging.getLogger(__name__)


def get_queries():
    return connection.queries


class QuerySourceMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Ensure `queries_log` is initialized
        if not hasattr(connection, 'queries_log'):
            connection.queries_log = []

        # Register the query execution wrapper
        with connection.execute_wrapper(self.query_logging_wrapper):
            response = self.get_response(request)

        return response

    def query_logging_wrapper(self, execute, sql, params, many, context):
        # Extract stack trace
        stack_trace = traceback.extract_stack()

        # Clean and filter out irrelevant stack traces
        project_base_dir = Path(settings.BASE_DIR).resolve()
        cleaned_stack = [
            frame for frame in stack_trace
            if str(project_base_dir) in frame.filename and 'middleware.py' not in frame.filename
        ]

        # Execute the query
        result = execute(sql, params, many, context)

        # Log the query along with the stack trace into `queries_log`
        connection.queries_log.append({
            'sql': sql,
            'params': params,
            'stack_trace': cleaned_stack,
            'time': '0.04',
        })

        return result

class QueryInspectorMiddleware:
    """Middleware to log SQL queries and their execution times per request."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):

        if not settings.DEBUG:
            # Only activate middleware in DEBUG mode
            logger.info('Turn on DEBUG mode to activate Query Inspector middleware')
            return self.get_response(request)

        if request.path.startswith(settings.STATIC_URL) or request.path.startswith(settings.MEDIA_URL):
            # Ignore static and media URLs
            return self.get_response(request)

        # Timer for the total request execution time
        start_time = time.time()

        # Before request (middleware processing)
        response = self.get_response(request)
                  
        # After request
        total_time = time.time() - start_time

        # Log query details after the response
        self.log_queries(total_time)

        return response
    
    def log_no_queries(self, total_time):
        logger.info(
            "No database interactions occurred during this request. Total Request Time: %.3f seconds", total_time)

    def log_queries(self, total_time):
        try:
            queries = connection.queries
            total_queries = len(queries)
            total_query_time = sum(float(query['time']) for query in queries)

            if queries:
                # General info about total queries and execution time
                logger.info(
                    "\n--- General info ---\n"
                    f"Total Queries: {total_queries}\n"
                    f"Total Query Execution Time: {total_query_time} s\n"
                    f"Total Request Time: {total_time} s\n"
                    "\n--------------------------------\n"
                )

                # Flag slow queries as a warning
                self.flag_slow_queries(queries)

                # Group and log repeated queries
                query_count = self.group_queries(queries)
                self.log_query_summary(query_count)

                # Pass queries to the QueryAnalyzer for inefficiency detection
                analyzer = QueryAnalyzer(queries)
                inefficiencies = analyzer.analyze()

                # Log detected inefficiencies
                self.log_inefficiencies(inefficiencies)

            else:
                self.log_no_queries(total_time)

        except DatabaseError as e:
            logger.error("Database error occurred: %s", str(e))
        except OperationalError as e:
            logger.error("Operational error occurred: %s", str(e))
        except Exception as e:
            logger.error("An unexpected error occurred while logging queries: %s", str(e))

    def group_queries(self, queries):
        """
        Groups queries by SQL string to count repeats and sum execution time.
        """
        query_count = {}
        for query in queries:
            sql = query['sql']
            if sql in query_count:
                query_count[sql]['count'] += 1
                query_count[sql]['total_time'] += float(query['time'])
            else:
                query_count[sql] = {
                    'count': 1,
                    'total_time': float(query['time'])
                }
        return query_count

    def log_query_summary(self, query_count):
        """
        Logs a summary of the queries, highlighting repeated queries.
        """
        logged_queries = set()

        for sql, details in query_count.items():
            # Skip if query has already been logged
            if sql in logged_queries:
                continue
        
            # Add the query to logged set before logging
            logged_queries.add(sql)
        
            # Format and log repeated queries
            if details['count'] > 1:
                logger.warning(
                    "\n--- Repeated Query ---\n"
                    f"SQL: {sql[:500]}\n"  # Truncate long SQL queries to 500 chars
                    f"Executed: {details['count']} times\n"
                    f"Total Execution Time: {details['total_time']} s\n"
                    "\n--------------------------------\n"
                )
            else:
                # Log non-repeated queries with INFO level
                logger.info(
                    "\n--- Query ---\n"
                    f"SQL: {sql[:500]}\n"
                    f"Total Execution Time: {details['total_time']} s\n"
                    "\n--------------------------------\n"
            )

    def log_inefficiencies(self, inefficiencies):
        """
        Logs detected inefficiencies.
        """
        suggestion_handler = OptimizationSuggestion()

        # Track logged inefficiencies and missing stack traces avoiding duplicates
        logged_inefficiencies = set()
        logged_missing_stack_trace = set()
        stack_trace = []

        for inefficiency in inefficiencies:
            inefficiency_type = inefficiency['type']
            query = inefficiency['query']
            count = inefficiency.get('count', 1)
            suggestion = inefficiency.get('suggestion', 'No suggestion available')

            if inefficiency_type in logged_inefficiencies:
                continue  # Skip already logged inefficiency

            # Find the corresponding query log with stack trace
            for logged_query in connection.queries_log:
                if logged_query:
                    if logged_query['sql'] == query:
                        stack_trace = logged_query.get('stack_trace', [])
                        break
                    else:
                        if logged_query['sql'] in logged_missing_stack_trace:
                            continue
                        else:
                            logger.warning(
                                "\n--- Missing Stack Trace---\n"
                                f"Sql: {logged_query['sql']} 'Missing stack trace'\n"
                                "--------------------------------\n"
                            )
                        logged_missing_stack_trace.add(logged_query['sql'])
                else:
                    logger.warning('Query not logged cannot therefore establish the stack trace')

            # Extract source from the stack trace
            source = stack_trace[0] if stack_trace else 'Unknown'

            # Truncate long query logs
            if len(query) > 100:
                query = f"{query[:100]} ... [truncated]"

            # Log inefficiency details
            logger.suggestion(
                "\n--- Inefficiency Detected ---\n"
                f"Type: {inefficiency_type}\n"
                f"Occurrences: {count}\n"
                f"Problematic Query:\n{query}\n"
                f"Source: {source}\n"
                "--------------------------------\n"
            )

            # Generate and log suggestion
            generated_suggestion = suggestion_handler.suggest({
                'type': inefficiency_type,
                'query': query,
                'suggestion': suggestion
            })

            logger.tip(
                "\n--- Optimization Tip ---\n"
                f" {generated_suggestion}\n"
                "--------------------------------\n"
            )

            # Mark this query as logged
            logged_inefficiencies.add(inefficiency_type)

    def flag_slow_queries(self, queries, threshold=None):
        # Log and flag slow queries that exceed the threshold (in seconds).
        if threshold is None:
            threshold = getattr(settings, 'SLOW_QUERY_THRESHOLD', 0.5)
            timeout_threshold = getattr(settings, 'TIMEOUT_THRESHOLD', 5)
        for query in queries:
            execution_time = float(query['time'])
            sql = query['sql']

            if execution_time > threshold:
                logger.warning(
                    "\n--- Slow Query---\n"
                    f"Sql: {sql}'Slow Query'\n"
                    f"Execution Time: {execution_time} ms\n"
                    "--------------------------------\n"
                )

            if execution_time > timeout_threshold:
                logger.error(
                    "\n--- Timeout error ---\n"
                    f"Query: {sql}\n"
                    f"Execution Time: {execution_time}\n"
                    "--------------------------------\n"
                )