import os
import re
import pandas as pd

from dotenv import find_dotenv, load_dotenv

from helpers import extract_text_from_pdfs

load_dotenv(find_dotenv())


def parse_idfc_wow_transaction_line(line):
    date_pattern = r'^\d{2}/\d{2}/\d{4}'
    match = re.match(date_pattern, line)
    if not match:
        return None

    parts = line.split()
    date = parts[0].replace('/', '-')
    amount = parts[-2] if parts[-1] == 'CR' else parts[-1]
    transaction_type = 'CREDIT' if parts[-1] == 'CR' else 'DEBIT'
    description = ' '.join(parts[1:-2]) if transaction_type == 'CREDIT' else ' '.join(parts[1:-1])
    return date, description, abs(float(amount.replace(',', ''))), transaction_type


def process_idfc_wow():
    idfc_path = 'data/idfc_wow'
    idfc_pdf_password = os.getenv('IDFC_WOW_PDF_PASSWORD')
    content = extract_text_from_pdfs(directory=idfc_path, password=idfc_pdf_password, page_number=1)

    idfc_data = []
    for file_name, lines in content.items():
        for line in lines:
            parsed_line = parse_idfc_wow_transaction_line(line)
            if parsed_line:
                date, description, amount, transaction_type = parsed_line
                idfc_data.append({
                    "date": date,
                    "description": description,
                    "amount": abs(amount),
                    "transaction_type": transaction_type
                })

    idfc_data_df = pd.DataFrame(idfc_data)
    idfc_data_df.to_csv('processed_data/idfc_wow_transactions.csv', index=False)


def main():
    process_idfc_wow()


if __name__ == '__main__':
    main()
