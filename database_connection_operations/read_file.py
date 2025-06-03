
import csv
from more_itertools import chunked
file_path= 'customers.csv'

def read_csv_in_chunks(file_path, chunk_size=100):
    with open(file_path, mode='r', encoding='utf-8', newline='') as file:
        reader = csv.DictReader(file)  # You can also use csv.reader(file) if not using headers
        chunk = []

        for i, row in enumerate(reader, start=1):
            chunk.append(row)

            if i % chunk_size == 0:
                yield chunk  # Yield the chunk
                chunk = []   # Reset chunk

        # Yield remaining rows
        if chunk:
            yield chunk



