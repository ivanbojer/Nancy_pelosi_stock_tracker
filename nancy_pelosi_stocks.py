import csv, zipfile
import requests
import sqlite3
from pypdf import PdfReader

YEAR = "2024"
DB_NAME = "nancy_pelosi.db"
FOLDER = "documents"
ZIP_FILE_URL = f'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{YEAR}FD.ZIP'
PDF_FILE_URL = f'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{YEAR}/'


def export_db_to_csv(csv_filename, db_name=DB_NAME):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Query to select data
    query = "SELECT * FROM transactions"

    # Open a CSV file to write
    with open(csv_filename, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Write the column headers
        cursor.execute(query)
        headers = [description[0] for description in cursor.description]
        csvwriter.writerow(headers)

        # Write all rows of data
        csvwriter.writerows(cursor.fetchall())

    # Close the database connection
    conn.close()


def init_table(db_name=DB_NAME):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create table if it does not exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            first_name TEXT,
            last_name TEXT,
            date TEXT,
            doc_id TEXT,
            ticker TEXT, 
            description TEXT
        )
    ''')
    conn.commit()
    conn.close()


def insert_transaction_data(first_name, last_name, date, doc_id, ticker, description, db_name=DB_NAME):
    print(f"Adding to database: {date}: {last_name}, {first_name} {doc_id}")
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        
        # Insert a row into the trading table
        cursor.execute('''
            INSERT INTO transactions (first_name, last_name, date, doc_id, ticker, description)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (first_name, last_name, date, doc_id, ticker, description))
        
        # Commit the transaction
        conn.commit()
        
    except sqlite3.IntegrityError as e:
        print(f"Error inserting data: {e}")
    
    finally:
        # Close the connection
        conn.close()


def record_exists(doc_id, db_name=DB_NAME):
    """
    Check if a record with the same primary key (doc_id) already exists in the table.

    Args:
        doc_id (str): The primary key to check.
        db_name (str): The database file name.

    Returns:
        bool: True if the record exists, False otherwise.
    """
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        
        # Check if the record exists
        cursor.execute('''
            SELECT 1 FROM transactions WHERE doc_id = ?
        ''', (doc_id,))
        result = cursor.fetchone()
        
        return result is not None  # True if record exists, False otherwise
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
    
    finally:
        conn.close()


def extract_text_from_pdf(pdf_path):
    print(f'Extracting text from {pdf_path}')
    try:
        reader = PdfReader(pdf_path)
        # print(reader.metadata)
        text = ""
        for page in reader.pages:
            text += page.extract_text()

        return text.replace('\n', ' ').replace('\x00', '')
    except Exception as e:
        print(e)
        return None



def extract_transactions(pdf_text, doc_id):
    transactions=[]
    sp_index = pdf_text.find('SP')
    while sp_index > 0:
        ticker_start = pdf_text.find('(', sp_index)
        ticker_end = pdf_text.find(')', ticker_start)

        ticker = pdf_text[ticker_start+1:ticker_end]
        # print(f'ticker: {ticker}')

        desc_index_start = pdf_text.find('D:', ticker_end)
        if desc_index_start > 0:
            desc_index_end = pdf_text.find(".", desc_index_start+1)
            description_text = pdf_text[desc_index_start+3:desc_index_end+1]
            # print(f'ticker: {ticker}, description: {description_text}')

            transactions.append(
                {
                    "ticker": ticker,
                    "description": description_text,
                    "doc_id": doc_id
                }
            )

        sp_index = pdf_text.find('SP', sp_index+1)

    return transactions


def main():
    # Get ZIP file
    r = requests.get(ZIP_FILE_URL)
    zipfile_name = f'{YEAR}.zip'

    with open(zipfile_name, 'wb') as f:
        f.write(r.content)

    with zipfile.ZipFile(zipfile_name) as z:
        z.extractall(f'{FOLDER}/.')

    init_table()
    con = sqlite3.connect(DB_NAME)

    # Open File with list of all transactions. This file changes as more transactions are added but
    # it is based on YEAR
    with open(f'{FOLDER}/{YEAR}FD.txt') as f:
        for line in csv.reader(f, delimiter='\t'):
            # if line[1] == 'Pelosi':
            # print(line)
            first_name = line[1]
            last_name = line[2]
            date = line[7]
            doc_id = line[8]

            if (doc_id == 'DocID'):
                continue

            if not record_exists(doc_id=doc_id):
                print(f"Retrieving new data from: {PDF_FILE_URL}{doc_id}.pdf")
                r = requests.get(f"{PDF_FILE_URL}{doc_id}.pdf")

                with open(f"{FOLDER}/{doc_id}.pdf", 'wb') as pdf_file:
                    pdf_file.write(r.content)

                pdf_text = extract_text_from_pdf(f"{FOLDER}/{doc_id}.pdf")

                #some PDFs are corrupted so we gotta skip them losing some important data
                if not pdf_text:
                    print(f'###################### Doc ID {doc_id} corrupted!!')
                    insert_transaction_data(first_name=first_name,
                                            last_name=last_name,
                                            date=date,
                                            doc_id=doc_id,
                                            ticker="CORRUPTED",
                                            description="CORRUPTED")
                    continue

                transactions = extract_transactions(pdf_text, doc_id)
                print(transactions)

                for single_transaction in transactions:
                    insert_transaction_data( first_name=first_name,
                                        last_name=last_name,
                                        date=date,
                                        doc_id=doc_id,
                                        ticker=single_transaction['ticker'],
                                        description=single_transaction['description'])
            else:
                print(f"Data with doc_id: {doc_id} is already stored in the database.")


if __name__ == "__main__":
    # main()
    export_db_to_csv('nancy_pelosi.csv')