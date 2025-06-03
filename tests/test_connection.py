from unittest.mock import patch, MagicMock
from database_connection_operations.connection import database_connection

@patch('database_connection_operations.connection.mysql.connector.connect')
def test_connection(mock_connect):
    mock_conn = MagicMock()
    mock_conn.is_connected.return_value = True
    mock_connect.return_value = mock_conn

    conn = database_connection()
    assert conn is not None
    assert conn.is_connected()
    mock_connect.assert_called_once()