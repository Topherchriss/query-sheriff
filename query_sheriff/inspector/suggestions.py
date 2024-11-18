import re

class OptimizationSuggestion:
    def suggest(self, inefficiency):
        suggestions = {
            'N+1 Query': (
                "⚠️ **N+1 Query Detected**\n"
                "Multiple queries are executed to fetch related objects in this query.\n"
                "🚩 **Impact**: N+1 queries can significantly degrade performance, particularly with large datasets, leading to excessive database calls and increased latency.\n"
                "🛠 **Cause**: Appropriate optimizations for querying related objects are not employed.\n"
                "✅ **Recommendation**: {suggestion}\n"
            ),

            'Missing Index on WHERE': (
                "⚠️ **Missing Index Detected**\n"
                "The absence of an index can severely impact query performance. Consider the following steps:\n"
                "🛠 **Analyze the Query**: Identify columns in the WHERE clause, JOIN conditions, or ORDER BY clause that may be causing slow lookups.\n"
                "🛠 **Verify Using `EXPLAIN`**: Execute `EXPLAIN` on the query to determine if  the query conducts a `Seq Scan`, if it does it's a strong candidate for indexing.\n"
                "🛠 **Create Index**: Implement an index on the relevant column(s) with the SQL command: "
                "`CREATE INDEX index_name ON table_name(column_name);` or declare it in your model with `db_index=True`.\n"
                "✅ **Suggested Index**: {suggestion}\n"
                "🚩 **Best Practices**: Avoid indexing very small tables or columns with low cardinality, as they may not yield performance improvements."
            ),
            
            'Missing Index on JOIN': (
                "⚠️ **Missing Index Detected on Foreign Key Column in JOIN**\n"
                "JOIN operations involving foreign key columns without an index might lead to significant performance bottlenecks. "
                "Consider the following steps:\n"
                "🛠 **Analyze the JOIN Condition**: Identify the Foreign key column involved in the JOIN operation.\n"
                "🛠 **Verify Using `EXPLAIN`**: Run `EXPLAIN` on the query and verify the JOIN is causing a full table scan "
                "`Seq Scan`. If detected, adding an index is recommended.\n"
                "🛠 **Create Index**: Add an index on the foreign key column using the SQL command: "
                "`CREATE INDEX index_name ON table_name (column_name);` or declare it in your model with `db_index=True`.\n"
                "✅ **Suggested Index**: {suggestion}\n"
                "🚩 **Best Practices**: Ensure the indexed column is used frequently in JOINs, as indexing infrequent columns may not yield significant improvements."
            ),

            'Missing index on ORDER BY': (
                "⚠️ **Missing Index Detected on ORDER BY clause**\n"
                "ORDER BY operations involving columns without indexes might lead to significant performance bottlenecks. "
                "Consider the following steps:\n"
                "🛠 **Analyze the ORDER BY Condition**: Identify the columns involved in the ORDER BY operation.\n"
                "🛠 **Verify Using `EXPLAIN`**: Run `EXPLAIN` on the query and verify the ORDER BY is causing a full table scan "
                "`Seq Scan`. If detected, adding an index is recommended.\n"
                "🛠 **Create Index**: Add an index on the column or columns involved using the SQL command: "
                "`CREATE INDEX index_name ON table_name (column_name);` or declare it in your model with `db_index=True`.\n"
                "✅ **Suggested Index**: {suggestion}\n"
                "🚩 **Best Practices**: Ensure the indexed column is used frequently in ORDER BYs, as indexing infrequent columns may not yield significant improvements."
            ),

            'Missing Index on AGGREGATE': (
                "⚠️ **Missing Index on Aggregate Detected**\n"
                "Queries involving aggregate functions on unindexed columns might cause performance bottlenecks with large datasets. Consider the following steps to optimize performance:\n"
                "🛠 **Analyze the Query**: Check the column(s) involved in the aggregate function, (`SUM()`, `COUNT()`, etc) are prime candidates for indexing.\n"
                "🛠 **Verify Using `EXPLAIN`**: Run `EXPLAIN` on the query to identify if a full table scan (`Seq Scan`) is taking place. If the query scans the table for aggregation, it is likely inefficient.\n"
                "🛠 **Create Index**: Add an index to the column(s) used in the aggregate function with the SQL command: "
                "`CREATE INDEX index_name ON table_name(column_name);` or declare it in your model with `db_index=True`.\n"
                "✅ **Suggested Index**: {suggestion}\n"
                "🚩 **Best Practices**: Avoid creating indexes for columns with low cardinality as they may not lead to significant performance improvements.\n"
            ),

            'Overuse of Subqueries': (
                "⚠️ **Overuse of Subqueries Detected**\n"
                "The query makes excessive use of subqueries, which might be inefficient unless intentional.\n"
                "🛠 **Consider**: Rewriting the query using `JOIN` operations or Common Table Expressions (CTEs) for better performance.\n"
                "✅ **Recommendation**: {suggestion}\n"
                "🚩 **Best Practices**: Subqueries are useful in certain cases, but using JOINs or CTEs is more efficient for most queries, especially with larger datasets."
            ),

            'Cartesian Product in JOIN': (
                "⚠️ **Cartesian Product Detected in JOIN**\n"
                "The query performs a JOIN without specifying an ON condition, resulting in a Cartesian product. This might lead to a massive result set with performance issues.\n"
                "🛠 **Consider**: Adding appropriate JOIN conditions using `ON` or `USING` clauses to filter the result set preventing Cartesian product.\n"
                "✅ **Recommendation**: {suggestion}\n"
                "🚩 **Best Practices**: Avoid Cartesian products unless explicitly needed. Always use appropriate `ON` conditions when joining tables."
            ),

            'Inefficient Pagination': (
                "⚠️ **Inefficient Pagination Detected**\n"
                "The query uses a large OFFSET, which can cause performance degradation as the dataset grows.\n"
                "🛠 **Consider**: Using keyset pagination (cursor-based pagination) instead of OFFSET-based pagination.\n"
                "✅ **Recommendation**: {suggestion}\n"
                "🚩 **Best Practices**: Keyset pagination improves performance by avoiding large OFFSET scans. Consider tracking the last seen record for pagination."
            ),

            'Non-Sargable Query': (
                "⚠️ **Non-Sargable Query Detected**\n"
                "The query uses a function or expression on an indexed column, which prevents the use of indexes and might lead to full table scans.\n"
                "🛠 **Consider**: Rewriting the query to be sargable by avoiding the use of functions on indexed columns in the WHERE clause.\n"
                "✅ **Recommendation**: {suggestion}\n"
                "🚩 **Best Practices**: Use direct comparisons to allow indexes to work efficiently. Avoid applying functions on indexed columns in WHERE clauses."
            ),

            'Locking Issue': (
                "⚠️ **Locking Issue Detected**\n"
                "The query or transaction takes longer than expected, potentially causing locking issues or deadlocks.\n"
                "🛠 **Consider**: Optimizing the query or reducing the scope of the transaction to avoid long locks.\n"
                "✅ **Recommendation**: {suggestion}\n"
                "🚩 **Best Practices**: Keep transactions short and break long-running queries into smaller operations to reduce lock contention."
            ),

            'Overuse of Transactions': (
                "⚠️ **Overuse of Transactions Detected**\n"
                "The transaction is running for too long, which could cause lock contention and performance issues.\n"
                "🛠 **Consider**: Reducing the scope of the transaction or breaking it into smaller transactions.\n"
                "✅ **Recommendation**: {suggestion}\n"
                "🚩 **Best Practices**: Use transactions only when necessary and avoid wrapping non-critical operations in transaction blocks."
            ),

            'Slow Query': (
                "⚠️ **Slow Query Detected**\n"
                "The execution time for this query is longer than expected. Review the query's execution plan.\n"
                "🛠 **Consider**: Optimize by reducing joins, refining conditions or increasing the execution time threshold.\n"
                "✅ **Recommendation**: {suggestion}\n"
            ),
            'Duplicate Query': (
                "⚠️ **Duplicate Query Detected**\n"
                "This query is being executed multiple times. Consider caching the result or eliminating duplicate query calls to improve efficiency.\n"
                "✅ **Recommendation**: {suggestion}\n"
            ),
            'Unnecessary DISTINCT': (
                "⚠️ **Unnecessary DISTINCT Detected**\n"
                "The use of DISTINCT in this query may be redundant. Verify if DISTINCT is applied to fields that are already unique.\n"
                "🛠 **Removing DISTINCT**: Can simplify the query and enhance performance. Additionally, ensure that duplicates are not being introduced from other parts of the query (e.g., JOINs).\n"
                "✅ **Recommendation**: {suggestion}\n"
            ),
            'Missing LIMIT': (
                "⚠️ **Missing LIMIT Clause Detected**\n"
                "Omitting a LIMIT clause can lead to performance degradation due to processing large datasets.\n"
                "🛠 **Consider**: Adding a LIMIT clause to restrict the dataset size and improve performance.\n"
                "✅ **Recommendation**: {suggestion}\n"
            ),
            'Full Table Scan': (
                "⚠️ **Full Table Scan Detected**\n"
                "Full table scans can be inefficient, particularly on large tables.\n"
                "🛠 **Utilize**: WHERE clause to filter rows can significantly enhance query performance.\n"
                "✅ **Recommendation**: {suggestion}\n"
            ),
            'Inefficient SELECT *': (
                "⚠️ **Inefficient SELECT * Detected**\n"
                "Using SELECT * retrieves all columns from the table, which can be inefficient.\n"
                "🛠 **Consider**: Specify only the necessary columns to reduce data transfer and improve performance.\n"
                "✅ **Recommendation**: {suggestion}\n"
            ),
        }

        # Insert the index suggestion into the message if it exists
        return suggestions.get(inefficiency['type'], "No suggestion available.").format(suggestion=inefficiency.get('suggestion', 'No suggestion available'))


### Optimization suggestion ###

def suggest_optimization_for_slow_query(sql):
    """
    Suggest optimizations for slow queries based on the SQL pattern.
    """
    suggestions = []
    
    # Check for JOINs or subqueries
    if "JOIN" in sql.upper():
        suggestions.append("Consider optimizing the JOIN conditions or adding indexes to the joined columns.")
    
    if "SELECT *" in sql.upper():
        suggestions.append("Avoid using SELECT *; select only the needed columns to reduce data retrieval.")
    
    suggestions.append("Consider caching this query if the result doesn't change frequently.")
    
    return ' '.join(suggestions)

def suggest_optimization_for_duplicate_query(sql, count):
    """
    Suggest optimizations for duplicate queries, such as caching or batching.
    """
    suggestions = []
    
    if count > 1:
        suggestions.append("Consider caching the result of this query to avoid redundant executions.")
    
    return ' '.join(suggestions)

def suggest_removing_distinct(selected_columns):
    """
    Suggest removing DISTINCT from the query if it's unnecessary.
    """
    # Ensure the selected_columns is a list of normalized columns
    if isinstance(selected_columns, str):
        # Split the string into columns in case it's not a list already
        selected_columns = [col.strip() for col in selected_columns.split(',')]

    return (
        "The DISTINCT clause in this query may be unnecessary. "
        f"The selected fields ({', '.join(selected_columns)}) are unique, so the DISTINCT clause may be redundant. "
        "Consider removing DISTINCT to simplify the query and improve performance."
    )

def suggest_limit_clause(sql):
    """
    Suggestion for missing LIMIT clause.
    """
    return "Consider adding a LIMIT clause to avoid returning large datasets. For example: 'SELECT ... LIMIT 100'."

# suggest WHERE clause
def suggest_where_clause(sql):
    """
    Suggestion for missing WHERE clause.
    """
    return "Consider adding a WHERE clause to filter the data and avoid a full table scan. For example: 'SELECT ... WHERE condition'."

# suggest specific column selection instead of SELECT *
def suggest_specific_columns(sql):
    """
    Suggestion for avoiding SELECT *.
    """
    return "Avoid using 'SELECT *'. Specify only the columns you need, such as 'SELECT column1, column2 FROM ...'."

def suggest_alternative_to_subquery(sql):
    """
    Suggest an alternative to using subqueries, like JOINs or CTEs.
    """
    # If the subquery is correlated suggest use of JOIN
    if re.search(r'WHERE.*IN\s*\(SELECT.*', sql, re.IGNORECASE):
        return (
            "Consider replacing the correlated subquery with a JOIN. Using subqueries in the WHERE clause might lead to inefficient execution plans.\n"
            "For example:\n"
            "Instead of: SELECT * FROM table WHERE column IN (SELECT column FROM ...)\n"
            "Use: SELECT t1.* FROM table t1 JOIN (SELECT column FROM ...) AS t2 ON t1.column = t2.column"
        )

    # Suggest using CTEs if subqueries are used repeatedly
    if sql.lower().count('(select') > 1:
        return (
            "Consider using a CTE to replace the subquery for better readability and performance.\n"
            "For example:\n"
            "WITH subquery AS (SELECT ... FROM ...) SELECT * FROM subquery JOIN ..."
        )

    # Default suggestion
    return (
        "Consider using JOINs or CTEs to replace subqueries. Subqueries can often be optimized by rewriting them as joins."
    )

def suggest_alternative_to_cartesian(sql):
    """
    Suggest an alternative to avoid cartesian products in JOINs.
    """
    # Suggest adding ON conditions or filtering joins
    if 'CROSS JOIN' in sql.upper():
        return (
            "CROSS JOIN results in a Cartesian product. Ensure that this is intentional. If not, add appropriate JOIN conditions "
            "(e.g., INNER JOIN or LEFT JOIN with ON conditions) to limit the result set."
        )

    # Default suggestion for join without ON condition
    return (
        "A JOIN without an ON condition might lead to a Cartesian product, causing a large result set. "
        "Include appropriate JOIN conditions (ON or USING) to join on related keys."
    )

def suggest_alternative_pagination():
    """
    Suggest using keyset (cursor-based) pagination instead of OFFSET-based pagination.
    """
    return (
        "Consider using keyset pagination (cursor-based pagination) instead of OFFSET-based pagination. "
        "OFFSET can cause performance degradation on large datasets as the OFFSET value increases. "
        "With keyset pagination, you can paginate more efficiently by using WHERE conditions and tracking the last seen record."
    )

def suggest_sargable_query():
    """
    Suggest rewriting the query to make it sargable.
    """
    return (
        "Rewriting the query to make it sargable will allow it to leverage indexes. "
        "Avoid using functions or expressions on indexed columns in the WHERE clause. "
        "Instead, rewrite the query to allow direct comparison (e.g., avoid using `WHERE FUNCTION(column)`; instead use `WHERE column = value`)."
    )

def suggest_avoiding_locks(execution_time):
    """
    Suggest avoiding long-running queries that can cause locks.
    """
    return (
        f"Query execution time of {execution_time:.2f} seconds exceeds the lock threshold. "
        "Consider optimizing the query or breaking it into smaller transactions to avoid long locks."
    )

def suggest_lock_optimization():
    """
    Suggest optimization for explicit LOCK statements.
    """
    return (
        "Avoid using explicit LOCK statements unless necessary. "
        "Consider using appropriate isolation levels or reducing the scope of the lock to avoid performance bottlenecks."
    )

def suggest_transaction_optimization(execution_time):
    """
    Suggest reducing the scope or optimizing the transaction.
    """
    return (
        f"The transaction running for {execution_time:.2f} seconds might be too long. "
        "Consider reducing the scope of the transaction or splitting it into smaller transactions "
        "to reduce lock contention and improve performance."
    )
