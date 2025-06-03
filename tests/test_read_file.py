
import pytest
import tempfile
import csv
import os
from database_connection_operations.read_file import read_csv_in_chunks

@pytest.fixture
def sample_csv_file():
    temp = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', suffix='.csv')
    writer = csv.DictWriter(temp, fieldnames=['id', 'name'])
    writer.writeheader()
    for i in range(1, 251):
        writer.writerow({'id': i, 'name': f'Name{i}'})
    temp.seek(0)
    temp.close()
    yield temp.name
    os.remove(temp.name)

def test_csv_in_chunks(sample_csv_file):
    chunks = list(read_csv_in_chunks(sample_csv_file, chunk_size=100))
    assert len(chunks) == 3
    assert len(chunks[0]) == 100
    assert len(chunks[1]) == 100
    assert len(chunks[2]) == 50
    assert chunks[0][0]['id'] == '1'
    assert chunks[2][-1]['id'] == '250'
