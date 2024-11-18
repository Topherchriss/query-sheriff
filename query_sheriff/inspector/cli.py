import sys
import os
import django # type: ignore
import click # type: ignore
from django.db import connection, DatabaseError # type: ignore
from query_sheriff.inspector.utils import WriteInefficienciesToFile # type: ignore
from .analyzer import QueryAnalyzer, QueryFetcher


@click.group()
def cli():
    """Command-line interface for Django Query Inspector."""
    pass

@click.command()
@click.option('--log-file', type=click.Path(exists=True), help="Path to the log file containing SQL queries.")
@click.option('--log-to-file', type=click.Path(), required=False, help="File to log inefficiencies to (required when using --log-file).")
@click.option('--settings', help='Path to Django settings module, e.g., "my_project.settings"')
@click.option('--sql-query', multiple=True, help="Raw SQL query or queries to analyze. You can pass multiple queries using this option.") # Accept multiple raw SQL queries
@click.argument('view_name', required=False)
def inspect_queries(log_file, log_to_file, view_name, sql_query, settings):
    """
    Fetch queries from either a log file, simulate a request to a view, or directly analyze raw SQL queries.
    """

    # Define the project's base directory
    project_base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))

    # Add the project's root directory to the system path
    if project_base_dir not in sys.path:
        sys.path.insert(0, project_base_dir)

    # Set the environment variable if `settings` is provided
    if settings:
        os.environ['DJANGO_SETTINGS_MODULE'] = settings
    elif not os.environ.get('DJANGO_SETTINGS_MODULE'):
        raise ValueError("DJANGO_SETTINGS_MODULE is not set. Use --settings option to provide the settings module.")

    # Initialize Django
    django.setup()

    # Ensure log-to-file is provided when log-file is provided
    if log_file and not log_to_file:
        click.echo(click.style("Error: --log-to-file is required when using --log-file option.", fg="red"))
        return

    # Only one query source is used at a time
    if log_file:
        if view_name or sql_query:
            click.echo(click.style("Error: Cannot use --log-file with view_name or raw SQL queries.", fg="red"))
            return

        # Fetch queries from a log file
        queries_to_analyze = QueryFetcher.fetch_from_log_file(log_file)

        if not test_database_connection():
            return

        # Analyze the queries
        analyze_queries(queries_to_analyze, log_to_file)

    elif view_name:
        if log_file or sql_query:
            click.echo(click.style("Error: Cannot use view_name with --log-file or raw SQL queries.", fg="red"))
            return

        # Simulate request, middleware handles logging and analysis
        click.echo(f"Simulating request to view '{view_name}'...")
        queries_to_analyze = QueryFetcher.fetch_from_simulated_request(view_name)
        return

    elif sql_query:
        if log_file or view_name:
            click.echo(click.style("Error: Cannot use raw SQL queries with --log-file or view_name.", fg="red"))
            return

        # Fetch and analyze raw SQL queries
        queries_to_analyze = QueryFetcher.fetch_from_manual_input(sql_query)

        if not test_database_connection():
            return

        # Analyze the queries
        analyze_queries(queries_to_analyze, log_to_file)

    else:
        click.echo(click.style("Please provide either --log-file, a view_name, or raw SQL queries.", fg="red"))
        return

def analyze_queries(queries, log_to_file=None):
    """
    Analyzes the queries and logs inefficiencies to either a file or console.
    """
    analyzer = QueryAnalyzer(queries)
    inefficiencies = analyzer.analyze()
    logged_inefficiencies = set()

    if inefficiencies:
        if log_to_file:
            WriteInefficienciesToFile().log_inefficiencies_to_file(inefficiencies, log_to_file)
        else:
            click.echo(click.style("Inefficiencies detected:", fg="yellow"))
            for inefficiency in inefficiencies:
                if inefficiency['type'] in logged_inefficiencies:
                    continue 
                click.echo(click.style(f"- {inefficiency['type']}: {inefficiency['query']}", fg='yellow'))
                click.echo(click.style(f"  Suggestion: {inefficiency['suggestion']}", fg='cyan'))
                logged_inefficiencies.add(inefficiency['type'])
            click.echo(click.style("TIP: Provide --log-to-file for detailed analysis and logging", fg="green"))
    else:
        click.echo(click.style("No inefficiencies detected.", fg="green"))

def test_database_connection():
    """
    Test if a valid database connection exists.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")  # test the connection
        return True
    except DatabaseError as e:
        click.echo(click.style(f"Error: Unable to connect to the database. {str(e)}", fg="red"))
        return False

# Add the command to the CLI group
cli.add_command(inspect_queries)

if __name__ == '__main__':
    cli()