import pytest
from unittest.mock import patch, MagicMock
from database_connection_operations.database_operation import insert_data_into_table
# from database_connection_operations.connection import database_/connection


@patch('database_connection_operations.database_operation.database_connection')
@patch('database_connection_operations.database_operation.read_csv_in_chunks')
def test_db_connection_failure(mock_read_chunks, mock_db_conn):
    mock_db_conn.return_value = None
    insert_data_into_table()
    mock_read_chunks.assert_not_called()

@patch('database_connection_operations.database_operation.database_connection')
@patch('database_connection_operations.database_operation.read_csv_in_chunks')
def test_successful_insertion(mock_read_chunks, mock_db_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_db_conn.return_value = mock_conn

    mock_chunk = [{
        'Index': 1, 'Customer Id': 123, 'First Name': 'John', 'Last Name': 'Doe',
        'Company': 'ABC', 'City': 'NY', 'Country': 'USA', 'Phone 1': '123456',
        'Phone 2': '789101', 'Email': 'john@example.com', 'Subscription Date': '2023-01-01',
        'Website': 'example.com'
    }]
    mock_read_chunks.return_value = [mock_chunk]

    insert_data_into_table()

    assert mock_cursor.executemany.called
    assert mock_conn.commit.called

@patch('database_connection_operations.database_operation.database_connection')
@patch('database_connection_operations.database_operation.read_csv_in_chunks')
def test_insertion_failure(mock_read_chunks, mock_db_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.executemany.side_effect = Exception("Insert failed")
    mock_conn.cursor.return_value = mock_cursor
    mock_db_conn.return_value = mock_conn

    mock_chunk = [{
        'Index': 1, 'Customer Id': 123, 'First Name': 'Jane', 'Last Name': 'Doe',
        'Company': 'XYZ', 'City': 'LA', 'Country': 'USA', 'Phone 1': '000000',
        'Phone 2': '111111', 'Email': 'jane@example.com', 'Subscription Date': '2023-02-01',
        'Website': 'test.com'
    }]
    mock_read_chunks.return_value = [mock_chunk]

    insert_data_into_table()

    assert mock_conn.rollback.called
