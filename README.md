## Overview
Query-Sheriff is a Django tool designed to inspect and optimize ORM queries, making it easy to identify performance inefficiencies and potential bottlenecks in development or pre-deployment stages. It empowers developers to ensure their applications are performant, scalable, and optimized before reaching production.

## Why use Query-Sheriff?
Django ORM can inadvertently introduce inefficiencies if left unchecked. Query-Sheriff automates query analysis, helping developers detect common issues in ORM usage and offer actionable optimization suggestions. This saves valuable time and helps prevent performance issues before they become a problem in production.

## Features
- Detects Common ORM Inefficiencies: N+1 queries, missing indexes, Cartesian products, and more.
- Middleware Integration: Captures and analyzes queries on-the-fly.
- CLI Commands: Allows manual query inspection and report generation.
- Detailed Optimization Suggestions: Provides clear, actionable tips for improving detected issues.

## Requirements
- Python 3.8+
- Django 3.2, 5.x
- click library for CLI functionality

## Installation
```bash
pip install query-sheriff
```

## Configure Middleware
To enable query inspection, add QuerySourceMiddleware and QueryInspectorMiddleware to your MIDDLEWARE settings respectivley:

```python
DEBUG = True  # Ensure DEBUG is enabled to activate middleware

MIDDLEWARE = [
...
'query_sheriff.inspector.middleware.QuerySourceMiddleware',
'query_sheriff.inspector.middleware.QueryInspectorMiddleware',
...
]

# You can configure additional settings to customize performance thresholds defauled to;

SLOW_QUERY_THRESHOLD = 0.5  # Time in seconds; queries longer than this are flagged
OFFSET_THRESHOLD = 500      # Large OFFSET values considered inefficient
LOCK_THRESHOLD = 5.0        # Time in seconds for locking issues detection
TRANSACTION_THRESHOLD = 5.0 # Time in seconds for long transaction warnings
SMALL_TABLE_THRESHOLD = 100 # Raws to consider 'small tables'

# Set up logging. 
LOGGING = {
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {message}',
            'style': '{',
        },
        'colored': {
            '()': 'colorlog.ColoredFormatter',
            'format': '%(log_color)s%(levelname)s %(message)s',
            'log_colors': {
                'DEBUG': 'bold_blue',
                'INFO': 'green',
                'TIP': 'cyan',
                'SUGGESTION': 'bold_yellow',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            },
            'style': '%',
        },
    },
    'handlers': {
        'file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': 'your/file/name',
            'formatter': 'verbose',
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'colored',
        },
    },
}
```

## Using the Middleware
Once set up, the middleware automatically logs SQL queries executed during a request-response cycle:

- Activate DEBUG mode to enable query inspection.
- Send a request to your Django application.
- Check the console or you log files for query analysis and optimization tips.

## CLI Commands
Use the query-inspector command to analyze queries from the command line:

Command Structure:

```bash
query-inspector inspect-queries --log-file=path/to/logfile.log --log-to-file=path/to/output.log --settings=your_project.settings
query-inspector inspect-queries --sql-query="SELECT * FROM ..." --sql-query="SELECT id FROM users ..." --log-to-file=path/to/output.log --settings=your_project.settings
query-inspector inspect-queries path/to/view/ --settings=your_project.settings
```

## Options
- --log-file: Path to the log file with SQL queries.
- --log-to-file: Output file for logging inefficiencies. Mandatory when --log-file is used.
- --settings: Specify the Django settings module (or use the environment variable 
```bash
DJANGO_SETTINGS_MODULE=your_project.settings
```
).
- --sql-query: Directly pass raw SQL queries for analysis. Log-to-file is optional here.
- path/to/view/: Simulate a request to a specific Django view. Log-to-file NOT required here.

## Note
The above commands might or might not work depending on the structure of your project. If they don’t work as expected, you can add a custom management command and use that as the entry point to the CLI. In your management_commands/managment/commands directory create a new file inspect_queries;

```python
# management_commands/managment/commands/inspect_queries.py
from django.core.management.base import BaseCommand
from click import Context
from query_sheriff.inspector.cli import cli

class Command(BaseCommand):
    help = 'Inspect queries using Query Sheriff'

    def add_arguments(self, parser):
        parser.add_argument('--log-file', type=str, help="Path to the log file")
        parser.add_argument('--log-to-file', type=str, help="File to log inefficiencies")
        parser.add_argument('--sql-query', nargs='+', help="Raw SQL queries")
        parser.add_argument('view_name', nargs='?', help="View name for simulation")

    def handle(self, *args, **options):
        with Context(cli) as ctx:
            ctx.invoke(
                cli.commands['inspect-queries'], 
                log_file=options.get('log_file'),
                log_to_file=options.get('log_to_file'),
                sql_query=options.get('sql_query'),
                view_name=options.get('view_name')
            )

```

```bash
python3 manage.py inspect_queries --log-file=path/to/logfile.log --log-to-file=path/to/output.log --settings=your_project.settings
python3 manage.py inspect_queries --sql-query="SELECT * FROM ..." --sql-query="SELECT id FROM users ..." --log-to-file=path/to/output.log --settings=your_project.settings
python3 manage.py inspect_queries path/to/view --settings=your_project.settings
```

## Expected output from the CLI options:

```log
--- Inefficiency Detected at 2024-11-04 12:24:54 ---
Type: Cartesian Product in JOIN
Occurrences: 1
Problematic Query:
SELECT im.name, rm.related_field FROM test_inspector_inspectormiddlewaremodel im JOIN test_inspector_relatedmodel rm ON im.id = rm.inspector_middleware_id; ... [truncated]
Source: Unknown
--------------------------------

--- Optimization Tip ---
 ⚠️ **Cartesian Product Detected in JOIN**
The query performs a JOIN without specifying an ON condition, resulting in a Cartesian product. This might lead to a massive result set with performance issues.
🛠 **Consider**: Adding appropriate JOIN conditions using `ON` or `USING` clauses to filter the result set preventing Cartesian product.
✅ **Recommendation**: A JOIN without an ON condition might lead to a Cartesian product, causing a large result set. Include appropriate JOIN conditions (ON or USING) to join on related keys.
🚩 **Best Practices**: Avoid Cartesian products unless explicitly needed. Always use appropriate `ON` conditions when joining tables.
--------------------------------
```

Note: 
- When using --log-file or --sql-query options, the source defaults to "Unknown."
- When using path/to/view option, make sure to include
```python
ALLOWED_HOSTS = [
    ..
    'testserver',
    ..
]
```

## Expected output from the middleware and CLI option path/to/view.
```log
SUGGESTION 2024-11-11 10:00:33,678 middleware 
--- Inefficiency Detected ---
Type: N+1 Query
Occurrences: 2
Problematic Query:
SELECT "authentication_user"."id", "authentication_user"."password", "authentication_user"."last_log ... [truncated]
Source: <FrameSummary file /your/django/views.py, line 189 in post>
--------------------------------

TIP 2024-11-11 10:00:33,691 middleware 
--- Optimization Tip ---
 ⚠️ **N+1 Query Detected**
Multiple queries are executed to fetch related objects in this query.
🚩 **Impact**: N+1 queries can significantly degrade performance, particularly with large datasets, leading to excessive database calls and increased latency.
🛠 **Cause**: Appropriate optimizations for querying related objects are not employed.
✅ **Recommendation**: Consider using select_related to optimize this query.

```

### Contributing, Lincese and support 
Contributions are welcomed!

- Fork the repository on GitHub.

- Create a feature branch:
 ```bash
 git checkout -b my-new-feature.
 ```
- Commit your changes: 
```bash
git commit -am 'Add new feature'.
```
- Push to the branch: 
```bash
git push origin my-new-feature.
```
- Submit a pull request.

Query-Sheriff is licensed under the MIT License.

For questions or support, open an issue on the GitHub repository.

