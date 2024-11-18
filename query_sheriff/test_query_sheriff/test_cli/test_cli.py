import pytest # type: ignore
from click.testing import CliRunner # type: ignore
from unittest.mock import mock_open, patch
from query_sheriff.inspector.cli import cli, inspect_queries

@pytest.fixture
def runner():
    return CliRunner()

def test_missing_django_settings_module(runner):
    # Mock `click.Path.convert` to bypass file existence checks
    with patch('click.Path.convert', return_value='sample.log'), \
         patch.dict('os.environ', {}, clear=True):
        
        result = runner.invoke(inspect_queries, ['--log-file=sample.log', '--log-to-file=sample_log_to.log'])
        assert result.exit_code == 1
        # assert "DJANGO_SETTINGS_MODULE is not set. Use --settings option to provide the settings module." in result.output

def test_log_file_without_log_to_file(runner):
    with patch('click.Path.convert', return_value='sample.log'), \
         patch('os.path.exists', return_value=True):
        result = runner.invoke(inspect_queries, ['--log-file=sample.log'])
        assert result.exit_code == 0
        assert "Error: --log-to-file is required when using --log-file option." in result.output

def test_conflicting_options_log_file_and_view_name(runner):
    with patch('click.Path.convert', return_value='sample.log'), \
         patch('os.path.exists', return_value=True):
        result = runner.invoke(inspect_queries, ['--log-file=sample.log', 'log-to-file=log_to_file.log', 'view_name'])
        assert result.exit_code == 2
        assert "Got unexpected extra argument (view_name)" in result.output

def test_inspect_queries_with_valid_log_file_and_log_to_file(runner):
    with patch('os.path.exists', return_value=True), \
         patch('click.Path.convert', return_value='sample.log'), \
         patch('query_sheriff.inspector.cli.QueryFetcher.fetch_from_log_file', return_value=[{'sql': 'SELECT * FROM users'}]), \
         patch('query_sheriff.inspector.cli.test_database_connection', return_value=True), \
         patch('query_sheriff.inspector.cli.analyze_queries') as mock_analyze_queries:
        
        result = runner.invoke(inspect_queries, ['--log-file=sample.log', '--log-to-file=sample_log_to.log', '--settings=my_project.settings'])
        assert result.exit_code == 0
        mock_analyze_queries.assert_called_once()

def test_inspect_queries_with_sql_query(runner):
    with patch('query_sheriff.inspector.cli.QueryFetcher.fetch_from_manual_input', return_value=[{'sql': 'SELECT * FROM users'}]), \
         patch('query_sheriff.inspector.cli.test_database_connection', return_value=True), \
         patch('query_sheriff.inspector.cli.analyze_queries') as mock_analyze_queries:
        
        result = runner.invoke(inspect_queries, ['--sql-query=SELECT * FROM users', '--settings=my_project.settings'])
        assert result.exit_code == 0
        mock_analyze_queries.assert_called_once()

def test_no_source_provided(runner):
    result = runner.invoke(inspect_queries)
    assert result.exit_code == 0
    assert "Please provide either --log-file, a view_name, or raw SQL queries." in result.output


def test_invalid_log_file_path(runner):
    with patch('click.Path.convert', side_effect=FileNotFoundError), \
         patch.dict('os.environ', {'DJANGO_SETTINGS_MODULE': 'my_project.settings'}):
        
        result = runner.invoke(inspect_queries, ['--log-file=invalid_path.log', '--log-to-file=output.log'])
        assert result.exit_code == 1
        # assert "Error: The provided log file path does not exist." in result.output


def test_view_name_with_log_file(runner):
    with patch('click.Path.convert', return_value='sample.log'), \
         patch('os.path.exists', return_value=True):
        
        result = runner.invoke(inspect_queries, ['--log-file=sample.log', '--log-to-file=output.log', 'view_name'])
        assert result.exit_code == 0
        assert "Error: Cannot use --log-file with view_name or raw SQL queries." in result.output


def test_sql_query_with_view_name(runner):
    with patch('query_sheriff.inspector.cli.QueryFetcher.fetch_from_manual_input', return_value=[{'sql': 'SELECT * FROM users'}]), \
         patch('query_sheriff.inspector.cli.test_database_connection', return_value=True), \
         patch('query_sheriff.inspector.cli.analyze_queries') as mock_analyze_queries:
        
        result = runner.invoke(inspect_queries, ['--sql-query=SELECT * FROM users', 'view_name', '--settings=my_project.settings'])
        assert result.exit_code == 0
        print('Results output', result.output)
        assert "Error: Cannot use view_name with --log-file or raw SQL queries." in result.output


def test_invalid_database_connection(runner):
    with patch('os.path.exists', return_value=True), \
         patch('click.Path.convert', return_value='sample.log'), \
         patch('query_sheriff.inspector.cli.test_database_connection', return_value=False), \
         patch.dict('os.environ', {'DJANGO_SETTINGS_MODULE': 'my_project.settings'}):
        
        result = runner.invoke(inspect_queries, ['--log-file=sample.log', '--log-to-file=output.log'])
        assert result.exit_code == 1

def test_analyze_queries_without_inefficiencies(runner):
    with patch('os.path.exists', return_value=True), \
         patch('click.Path.convert', return_value='sample.log'), \
         patch('query_sheriff.inspector.cli.QueryFetcher.fetch_from_log_file', return_value=[{'sql': 'SELECT * FROM users'}]), \
         patch('query_sheriff.inspector.cli.test_database_connection', return_value=True), \
         patch('query_sheriff.inspector.cli.analyze_queries') as mock_analyze_queries:
        
        # Simulate analyze method returning no inefficiencies
        mock_analyze_queries.return_value = []
        
        result = runner.invoke(inspect_queries, ['--log-file=sample.log', '--log-to-file=output.log', '--settings=my_project.settings'])
        assert result.exit_code == 0
        # assert  result.output == None