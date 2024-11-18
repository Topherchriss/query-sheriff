import pytest # type: ignore
from unittest.mock import patch, mock_open
from django.conf import settings # type: ignore
from query_sheriff.inspector.analyzer import QueryAnalyzer, QueryFetcher


def test_detect_n_plus_one():
    # Mocked queries simulating N+1 query pattern with similar WHERE clauses
    queries = [
        {'sql': 'SELECT * FROM orders WHERE user_id = 1'},
        {'sql': 'SELECT * FROM orders WHERE user_id = 2'},
        {'sql': 'SELECT * FROM orders WHERE user_id = 3'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mocking `simplify_query` and `detect_repeated_queries_for_related_objects`
    with patch('query_sheriff.inspector.analyzer.simplify_query', side_effect=lambda sql: 'SELECT * FROM orders WHERE user_id = ?'), \
         patch('query_sheriff.inspector.analyzer.detect_repeated_queries_for_related_objects', return_value=[
            {'type': 'N+1 Query', 'query': 'SELECT * FROM orders', 'count': 3, 'suggestion': 'Use select_related.'}
         ]) as mock_detect:
        
        inefficiencies = analyzer.detect_n_plus_one()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] == 'N+1 Query'
        assert inefficiencies[0]['count'] == 3
        mock_detect.assert_called_once()

def test_detect_missing_indexes():
    # Mocked queries simulating missing indexes in WHERE clauses
    queries = [
        {'sql': 'SELECT * FROM users WHERE email = "test@example.com"'},
        {'sql': 'SELECT * FROM products WHERE category = "electronics"'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mocking `extract_table_and_column_names` and `detect_missing_indexes_for_clause`
    with patch('query_sheriff.inspector.analyzer.extract_table_and_column_names', return_value=(['users'], ['email'])), \
         patch('query_sheriff.inspector.analyzer.detect_missing_indexes_for_clause', return_value=[
             {'type': 'Missing Index on WHERE', 'query': 'SELECT * FROM users WHERE email = "test@example.com"', 'suggestion': 'Add index on users.email'}
         ]) as mock_detect:
        
        inefficiencies = analyzer.detect_missing_indexes()
        
        # Assertions
        assert len(inefficiencies) == 2
        assert inefficiencies[0]['type'] == 'Missing Index on WHERE'
        assert 'suggestion' in inefficiencies[0]
        mock_detect.assert_called()

def test_detect_join_without_index():
    # Mocked queries simulating joins without indexes on foreign keys
    queries = [
        {'sql': 'SELECT * FROM orders JOIN users ON orders.user_id = users.id'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `extract_joins_from_query`, `extract_table_and_column_names`, and `get_join_columns`
    with patch('query_sheriff.inspector.analyzer.extract_joins_from_query', return_value=[('orders', 'orders.user_id', 'users.id')]), \
         patch('query_sheriff.inspector.analyzer.extract_table_and_column_names', return_value=(['orders', 'users'], [])), \
         patch('query_sheriff.inspector.analyzer.get_join_columns', return_value=(['user_id'], ['id'])), \
         patch('query_sheriff.inspector.analyzer.detect_missing_indexes_for_clause', return_value=[
             {'type': 'Missing Index on JOIN', 'query': 'SELECT * FROM orders JOIN users ON orders.user_id = users.id', 'suggestion': 'Add index on orders.user_id'}
         ]) as mock_detect:
        
        inefficiencies = analyzer.detect_join_without_index()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] == 'Missing Index on JOIN'
        assert 'suggestion' in inefficiencies[0]
        mock_detect.assert_called_once()


def test_detect_inefficient_order_by():
    # Mocked queries with ORDER BY on non-indexed columns
    queries = [
        {'sql': 'SELECT * FROM users ORDER BY created_at'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `extract_table_names`, `extract_order_by_columns`, and `detect_missing_indexes_for_clause`
    with patch('query_sheriff.inspector.analyzer.extract_table_names', return_value=['users']), \
         patch('query_sheriff.inspector.analyzer.extract_order_by_columns', return_value=['created_at']), \
         patch('query_sheriff.inspector.analyzer.detect_missing_indexes_for_clause', return_value=[
             {'type': 'Missing Index on ORDER BY', 'query': 'SELECT * FROM users ORDER BY created_at', 'suggestion': 'Add index on users.created_at'}
         ]) as mock_detect:
        
        inefficiencies = analyzer.detect_inefficient_order_by()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] == 'Missing Index on ORDER BY'
        assert 'suggestion' in inefficiencies[0]
        mock_detect.assert_called_once()


def test_detect_inefficient_aggregates():
    # Mocked queries with aggregate functions on non-indexed columns
    queries = [
        {'sql': 'SELECT COUNT(*) FROM products WHERE price > 100'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `extract_table_names`, `extract_aggregate_functions`, and `detect_missing_indexes_for_clause`
    with patch('query_sheriff.inspector.analyzer.extract_table_names', return_value=['products']), \
         patch('query_sheriff.inspector.analyzer.extract_aggregate_functions', return_value=['price']), \
         patch('query_sheriff.inspector.analyzer.detect_missing_indexes_for_clause', return_value=[
             {'type': 'Missing Index on AGGREGATE', 'query': 'SELECT COUNT(*) FROM products WHERE price > 100', 'suggestion': 'Add index on products.price'}
         ]) as mock_detect:
        
        inefficiencies = analyzer.detect_inefficient_aggregates()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] == 'Missing Index on AGGREGATE'
        assert 'suggestion' in inefficiencies[0]
        mock_detect.assert_called_once()

def test_detect_unnecessary_distinct():
    # Mocked queries using DISTINCT unnecessarily
    queries = [
        {'sql': 'SELECT DISTINCT name FROM employees'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `extract_select_clause`, `extract_table_names`, and `is_distinct_unnecessary`
    with patch('query_sheriff.inspector.analyzer.extract_select_clause', return_value='name'), \
         patch('query_sheriff.inspector.analyzer.extract_table_names', return_value=['employees']), \
         patch('query_sheriff.inspector.analyzer.is_distinct_unnecessary', return_value=True), \
         patch('query_sheriff.inspector.analyzer.suggest_removing_distinct', return_value="Consider removing DISTINCT for better performance.") as mock_suggest:
        
        inefficiencies = analyzer.detect_unnecessary_distinct()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] == 'Unnecessary DISTINCT'
        assert 'suggestion' in inefficiencies[0]
        mock_suggest.assert_called_once()


def test_detect_innefficient_use_of_subqueries():
    # Mocked queries with subqueries
    queries = [
        {'sql': 'SELECT * FROM (SELECT id FROM orders) AS subquery'},
        {'sql': 'SELECT * FROM users WHERE EXISTS (SELECT id FROM orders WHERE user_id = users.id)'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `suggest_alternative_to_subquery`
    with patch('query_sheriff.inspector.analyzer.suggest_alternative_to_subquery',
               return_value="Consider using JOINs or CTEs.") as mock_suggest:
        
        inefficiencies = analyzer.detect_innefficient_use_of_subqueries()
        
        # Assertions
        assert len(inefficiencies) == 2
        assert inefficiencies[0]['type'] == 'Overuse of Subqueries'
        assert 'suggestion' in inefficiencies[0]
        mock_suggest.assert_called()

def test_detect_cartesian_product_in_joins():
    # Mocked queries with Cartesian product patterns in JOINs
    queries = [
        {'sql': 'SELECT * FROM users JOIN orders'},
        {'sql': 'SELECT * FROM users CROSS JOIN orders'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `suggest_alternative_to_cartesian`
    with patch('query_sheriff.inspector.analyzer.suggest_alternative_to_cartesian', 
               return_value="Consider adding JOIN conditions or avoiding CROSS JOIN.") as mock_suggest:
        
        inefficiencies = analyzer.detect_cartesian_product_in_joins()
        
        # Assertions
        assert len(inefficiencies) == 3
        assert inefficiencies[0]['type'] in ['Cartesian Product in JOIN', 'Cartesian Product in CROSS JOIN']
        assert 'suggestion' in inefficiencies[0]
        mock_suggest.assert_called()

def test_detect_slow_queries():
    # Mocked slow queries with execution times exceeding threshold
    queries = [
        {'sql': 'SELECT * FROM users', 'time': '1.0'},
        {'sql': 'SELECT * FROM orders', 'time': '0.6'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock settings for threshold and `suggest_optimization_for_slow_query`
    with patch.object(settings, 'SLOW_QUERY_THRESHOLD', 0.5), \
         patch('query_sheriff.inspector.analyzer.suggest_optimization_for_slow_query', return_value="Consider optimizing indexes or query structure.") as mock_suggest:
        
        inefficiencies = analyzer.detect_slow_queries()
        
        # Assertions
        assert len(inefficiencies) == 2
        assert inefficiencies[0]['type'] == 'Slow Query'
        assert 'suggestion' in inefficiencies[0]
        mock_suggest.assert_called()

def test_detect_duplicate_queries():
    # Mocked duplicate queries
    queries = [
        {'sql': 'SELECT * FROM users'},
        {'sql': 'SELECT * FROM users'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `suggest_optimization_for_duplicate_query`
    with patch('query_sheriff.inspector.analyzer.suggest_optimization_for_duplicate_query', 
               return_value="Consider caching results or optimizing query.") as mock_suggest:
        
        inefficiencies = analyzer.detect_duplicate_queries()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] == 'Duplicate Query'
        assert inefficiencies[0]['count'] == 2
        assert 'suggestion' in inefficiencies[0]
        mock_suggest.assert_called_once()

def test_detect_missing_limit():
    # Mocked queries missing LIMIT clause
    queries = [
        {'sql': 'SELECT * FROM users'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `suggest_limit_clause`
    with patch('query_sheriff.inspector.analyzer.suggest_limit_clause',
               return_value="Consider adding a LIMIT clause to restrict data size.") as mock_suggest:
        
        inefficiencies = analyzer.detect_missing_limit()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] == 'Missing LIMIT'
        assert 'suggestion' in inefficiencies[0]
        mock_suggest.assert_called_once()

def test_detect_full_table_scan():
    # Mocked queries causing full table scans
    queries = [
        {'sql': 'SELECT * FROM users'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `suggest_where_clause`
    with patch('query_sheriff.inspector.analyzer.suggest_where_clause',
               return_value="Consider adding a WHERE clause to limit data scanned.") as mock_suggest:
        
        inefficiencies = analyzer.detect_full_table_scan()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] == 'Full Table Scan'
        assert 'suggestion' in inefficiencies[0]
        mock_suggest.assert_called_once()

def test_detect_inefficient_select():
    # Mocked queries using SELECT * inefficiently
    queries = [
        {'sql': 'SELECT * FROM users'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `suggest_specific_columns`
    with patch('query_sheriff.inspector.analyzer.suggest_specific_columns',
               return_value="Specify columns explicitly instead of using SELECT *.") as mock_suggest:
        
        inefficiencies = analyzer.detect_inefficient_select()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] == 'Inefficient SELECT *'
        assert 'suggestion' in inefficiencies[0]
        mock_suggest.assert_called_once()

def test_detect_inefficient_pagination():
    # Mocked queries with large OFFSET value for inefficient pagination
    queries = [
        {'sql': 'SELECT * FROM users LIMIT 10 OFFSET 600'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `suggest_alternative_pagination`
    with patch('query_sheriff.inspector.analyzer.suggest_alternative_pagination', return_value="Consider using cursor-based pagination.") as mock_suggest, \
         patch.object(settings, 'OFFSET_THRESHOLD', 500):
        
        inefficiencies = analyzer.detect_inefficient_pagination()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] == 'Inefficient Pagination'
        assert 'suggestion' in inefficiencies[0]
        mock_suggest.assert_called_once()

def test_detect_non_sargable_queries():
    # Mocked non-sargable queries using functions in WHERE clause
    queries = [
        {'sql': 'SELECT * FROM users WHERE FUNCTION(column) = 1'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `suggest_sargable_query`
    with patch('query_sheriff.inspector.analyzer.suggest_sargable_query', return_value="Rewrite query to avoid non-sargable functions.") as mock_suggest:
        
        inefficiencies = analyzer.detect_non_sargable_queries()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] == 'Non-Sargable Query'
        assert 'suggestion' in inefficiencies[0]
        mock_suggest.assert_called_once()

def test_detect_locking_issues():
    # Mocked queries to simulate locking issues (long execution time and explicit LOCK)
    queries = [
        {'sql': 'SELECT * FROM orders'},
        {'sql': 'LOCK TABLE users IN ACCESS EXCLUSIVE MODE'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `suggest_avoiding_locks` and `suggest_lock_optimization`
    with patch('query_sheriff.inspector.analyzer.suggest_avoiding_locks', return_value="Optimize query to avoid locks."), \
         patch('query_sheriff.inspector.analyzer.suggest_lock_optimization', return_value="Avoid using explicit LOCK statements."), \
         patch.object(settings, 'LOCK_THRESHOLD', 5.0):
        
        inefficiencies = analyzer.detect_locking_issues()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] in ['Locking Issue', 'Explicit LOCK Statement']
        assert 'suggestion' in inefficiencies[0]

def test_detect_overuse_of_transactions():
    # Mocked queries simulating long-running transactions
    queries = [
        {'sql': 'BEGIN; SELECT * FROM users; COMMIT;', 'time': '6.0'}
    ]
    analyzer = QueryAnalyzer(queries)

    # Mock `suggest_transaction_optimization`
    with patch('query_sheriff.inspector.analyzer.suggest_transaction_optimization', return_value="Optimize transaction to reduce runtime.") as mock_suggest, \
         patch.object(settings, 'TRANSACTION_THRESHOLD', 5.0):
        
        inefficiencies = analyzer.detect_overuse_of_transactions()
        
        # Assertions
        assert len(inefficiencies) == 1
        assert inefficiencies[0]['type'] == 'Overuse of Transactions'
        assert 'suggestion' in inefficiencies[0]
        mock_suggest.assert_called_once()


# Test QueryFetcher 


query_log_file = 'query_sheriff/tests/tests/test_inspector/queries.log'

def test_fetch_from_log_file_valid():
    mock_log_content = 'SQL: SELECT * FROM users\nSQL: DELETE FROM orders\n'
    
    # Patch both `open` and `os.path.exists`
    with patch('builtins.open', mock_open(read_data=mock_log_content)), \
         patch('os.path.exists', return_value=True):  # Mock exists to return True
        
        # Now fetch from the mocked log file path
        queries = QueryFetcher.fetch_from_log_file('mock_log_file.txt')
        
        # Assert that the queries were parsed correctly
        assert len(queries) == 2
        assert queries[0]['sql'] == "SELECT * FROM users"
        assert queries[1]['sql'] == "DELETE FROM orders"

def test_fetch_from_log_file_invalid_sql():
    mock_log_content = 'SQL: SELECT * FROM users\nSQL: NOT_A_VALID_SQL\n'
    # Patch both `open` and `os.path.exists`
    with patch('builtins.open', mock_open(read_data=mock_log_content)), \
         patch('os.path.exists', return_value=True):
        
        queries = QueryFetcher.fetch_from_log_file('mock_log_file.txt')

        assert len(queries) == 1
        assert queries[0]['sql'] == "SELECT * FROM users"

"""
@patch('query_sheriff.inspector.analyzer.Client.get')
@patch('query_sheriff.inspector.middleware.connection.queries_log', new_callable=list)
def test_fetch_from_simulated_request(mock_queries_log, mock_get):
    mock_get.return_value.status_code = 200
    mock_queries_log.append({'sql': 'SELECT * FROM users'})
    queries = QueryFetcher.fetch_from_simulated_request('test_view')
    assert len(queries) == 1
    assert queries[0] == 'SELECT * FROM users'
"""
"""
class TestQueryFetcher(TestCase):
    def test_fetch_from_simulated_request(self):
        # Set up the client and request path
        client = Client()
        view_name = 'aggregation_view'  # Replace with an actual view name in your app

        # Make a request to the view and count the number of queries executed
        with self.assertNumQueries(1):
            response = client.get(f'/{view_name}/'.replace('_', '-'))
            assert response.status_code == 200, f"Expected status code 200 but got {response.status_code}"

            # Now call the function to retrieve queries
            queries = QueryFetcher.fetch_from_simulated_request(view_name)

            # Verify the queries output
            assert len(queries) == 1, f"Expected 1 query, but got {len(queries)}"
            assert "SELECT * FROM users" in queries[0], f"Expected query not found in {queries[0]}"
"""

@patch('query_sheriff.inspector.analyzer.Client.get')
def test_fetch_from_simulated_request_invalid_view(mock_get):
    mock_get.return_value.status_code = 404
    with pytest.raises(ValueError):
        QueryFetcher.fetch_from_simulated_request('invalid_view')

def test_fetch_from_manual_input():
    queries = ["SELECT * FROM users", "INVALID SQL"]
    results = QueryFetcher.fetch_from_manual_input(queries)
    assert len(results) == 1
    assert results[0]['sql'] == "SELECT * FROM users"

def test_is_valid_sql():
    assert QueryFetcher.is_valid_sql("SELECT * FROM users") is True
    assert QueryFetcher.is_valid_sql("INVALID SQL") is False
    assert QueryFetcher.is_valid_sql("CREATE TABLE test") is True
    assert QueryFetcher.is_valid_sql("DROP DATABASE test") is True