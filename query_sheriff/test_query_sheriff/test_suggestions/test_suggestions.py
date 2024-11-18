from query_sheriff.inspector.suggestions import OptimizationSuggestion


def test_suggestion_for_n_plus_one_query():
    # Set up an inefficiency dictionary for an N+1 query issue
    inefficiency = {
        'type': 'N+1 Query',
        'suggestion': 'Use select_related or prefetch_related for optimization.'
    }
    suggestion_instance = OptimizationSuggestion()
    suggestion_text = suggestion_instance.suggest(inefficiency)

    # Assertions
    assert "N+1 Query Detected" in suggestion_text
    assert "Use select_related or prefetch_related for optimization." in suggestion_text

def test_suggestion_for_missing_index_on_where():
    # Set up an inefficiency dictionary for a missing index on WHERE clause
    inefficiency = {
        'type': 'Missing Index on WHERE',
        'suggestion': 'Consider adding an index on the WHERE column for optimization.'
    }
    suggestion_instance = OptimizationSuggestion()
    suggestion_text = suggestion_instance.suggest(inefficiency)

    # Assertions
    assert "Missing Index Detected" in suggestion_text
    assert "Consider adding an index on the WHERE column for optimization." in suggestion_text

def test_suggestion_for_unknown_inefficiency_type():
    # Test unknown inefficiency type
    inefficiency = {
        'type': 'Unknown Inefficiency Type',
        'suggestion': 'N/A'
    }
    suggestion_instance = OptimizationSuggestion()
    suggestion_text = suggestion_instance.suggest(inefficiency)

    # Assertions
    assert suggestion_text == "No suggestion available."

def test_suggestion_for_missing_limit():
    # Set up an inefficiency dictionary for a missing LIMIT clause
    inefficiency = {
        'type': 'Missing LIMIT',
        'suggestion': 'Add a LIMIT clause to avoid large datasets.'
    }
    suggestion_instance = OptimizationSuggestion()
    suggestion_text = suggestion_instance.suggest(inefficiency)

    # Assertions
    assert "Missing LIMIT Clause Detected" in suggestion_text
    assert "Add a LIMIT clause to avoid large datasets." in suggestion_text

def test_suggestion_for_cartesian_product_in_join():
    # Set up an inefficiency dictionary for Cartesian product in JOIN
    inefficiency = {
        'type': 'Cartesian Product in JOIN',
        'suggestion': 'Specify an ON condition to avoid Cartesian product.'
    }
    suggestion_instance = OptimizationSuggestion()
    suggestion_text = suggestion_instance.suggest(inefficiency)

    # Assertions
    assert "Cartesian Product Detected in JOIN" in suggestion_text
    assert "Specify an ON condition to avoid Cartesian product." in suggestion_text

def test_suggestion_for_full_table_scan():
    # Set up an inefficiency dictionary for a full table scan
    inefficiency = {
        'type': 'Full Table Scan',
        'suggestion': 'Use a WHERE clause to filter rows.'
    }
    suggestion_instance = OptimizationSuggestion()
    suggestion_text = suggestion_instance.suggest(inefficiency)

    # Assertions
    assert "Full Table Scan Detected" in suggestion_text
    assert "Use a WHERE clause to filter rows." in suggestion_text

def test_missing_index_on_join_suggestion():
    inefficiency = {
        'type': 'Missing Index on JOIN',
        'suggestion': 'Index the `foreign_key_id` column for better JOIN performance.'
    }
    suggestion_instance = OptimizationSuggestion()
    result = suggestion_instance.suggest(inefficiency)

    assert '**Missing Index Detected on Foreign Key Column in JOIN**' in result
    assert '**Suggested Index**: Index the `foreign_key_id` column for better JOIN performance.' in result
    assert '**Analyze the JOIN Condition**: Identify the Foreign key column involved in the JOIN operation.' in result

def test_missing_index_on_order_by_suggestion():
    inefficiency ={
        'type': 'Missing index on ORDER BY', 
        'suggestion': 'Add an index to the `created_at` column for optimized sorting.'
    }
    suggestion_instance = OptimizationSuggestion()
    result = suggestion_instance.suggest(inefficiency)

    assert '**Missing Index Detected on ORDER BY clause**' in result
    assert '**Suggested Index**: Add an index to the `created_at` column for optimized sorting.' in result
    assert '**Create Index**' in result

def test_unknown_inefficiency():
    inefficiency = {
        'type': 'Unknown Issue',
        'suggestion': 'No known optimization available.'
    }
    suggestion_instance = OptimizationSuggestion()
    result = suggestion_instance.suggest(inefficiency)
    print(f'Result: {result}')
    # This should raise an error or return None as there's no suggestion defined for 'Unknown Issue'
    assert result is None or result == 'No suggestion available.'

def test_empty_suggestion_text():
    inefficiency = {
        'type': 'N+1 Query',
        'suggestion': ''
    }

    suggestion_instance = OptimizationSuggestion()
    result = suggestion_instance.suggest(inefficiency)

    assert '**Recommendation**: ' in result  # recommendation still renders even if empty

def test_edge_case_large_suggestion_text():
    inefficiency = {
        'type': 'Missing Index on WHERE',
        'suggestion': 'Optimize column with detailed strategy: ' + 'index' * 100
    }
    suggestion_instance = OptimizationSuggestion()
    result = suggestion_instance.suggest(inefficiency)

    assert 'Optimize column with detailed strategy:' in result
    assert 'index' * 10 in result