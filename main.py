import os
import re
from datetime import datetime

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
    idfc_data_df['credit_cards_name'] = idfc_path.split('/')[-1]
    idfc_data_df.to_csv('processed_data/idfc_wow_transactions.csv', index=False)


def parse_axis_bank_transaction_line(row):
    if pd.notnull(row['DEBIT']):
        return row['DEBIT'], 'DEBIT'
    elif pd.notnull(row['CREDIT']):
        return row['CREDIT'], 'CREDIT'
    else:
        raise Exception('unexpected row')


def process_axis_bank():
    axis_bank_path = 'data/axis_bank'
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame to store combined data
    date_pattern = re.compile(r'^\d{2}-\d{2}-\d{4}$')  # Regex pattern to match 'dd-mm-yyyy'

    for root, dirs, files in os.walk(axis_bank_path):
        for filename in files:
            if filename.endswith('.csv'):
                file_path = os.path.join(root, filename)
                print(f"Processing file: {file_path}")
                df = pd.read_csv(file_path, skiprows=18, header=0, na_values=['', ' '])
                df = df.dropna(thresh=df.shape[1] - 4)
                df = df[df.iloc[:, 0].apply(lambda x: bool(date_pattern.match(str(x))))]
                combined_df = pd.concat([combined_df, df], ignore_index=True)

    combined_df.columns = ["DATE", "CHQNO", "DESCRIPTION", "DEBIT", "CREDIT", "BAL", "SOL"]
    combined_df[['AMOUNT', 'TRANSACTION_TYPE']] = combined_df.apply(parse_axis_bank_transaction_line, axis=1, result_type='expand')
    formatted_df = combined_df[['DATE', 'DESCRIPTION', 'AMOUNT', 'TRANSACTION_TYPE']].copy()
    formatted_df.columns = [col.lower() for col in formatted_df.columns]
    formatted_df['credit_cards_name'] = axis_bank_path.split('/')[-1]

    formatted_df.to_csv('processed_data/axis_bank_transactions.csv', index=False)
    print("Combined DataFrame saved to 'axis_bank_transactions.csv'")


def process_axis_credit_cards():
    axis_credit_cards = ['data/axis_flipkart', 'data/axis_my_zone']
    # axis_my_zone_path = 'data/axis_my_zone'

    combined_df = pd.DataFrame()  # Initialize an empty DataFrame to store combined data
    date_pattern = re.compile(r'^\d{2} [A-Za-z]{3} \'\d{2}$')  # Regex pattern to match 'dd mmm 'yy'

    for card in axis_credit_cards:
        for root, dirs, files in os.walk(card):
            for filename in files:
                if filename.endswith('.xlsx'):
                    file_path = os.path.join(root, filename)
                    print(f"Processing file: {file_path}")
                    df = pd.read_excel(file_path, sheet_name='Transactions Summary')
                    df.columns = ['DATE', 'DESCRIPTION', 'DROP', 'AMOUNT', 'TRANSACTION_TYPE']
                    filtered_df = df[df['DATE'].apply(lambda x: bool(date_pattern.match(x)))]
                    formatted_df = filtered_df[['DATE', 'DESCRIPTION', 'AMOUNT', 'TRANSACTION_TYPE']].copy()
                    formatted_df.columns = [col.lower() for col in formatted_df.columns]

                    formatted_df['credit_cards_name'] = card.split('/')[-1]
                    formatted_df['date'] = formatted_df['date'].apply(lambda x: datetime.strptime(x, "%d %b '%y").strftime("%d-%m-%Y"))
                    formatted_df['amount'] = formatted_df['amount'].apply(lambda x: x.replace(',', '').replace('₹ ', ''))
                    formatted_df['transaction_type'] = formatted_df['transaction_type'].apply(lambda x: x.upper())

                    combined_df = pd.concat([combined_df, formatted_df], ignore_index=True)

    combined_df.to_csv('processed_data/axis_credit_cards_transactions.csv', index=False)


def main():
    print('Started')
    process_idfc_wow()
    process_axis_bank()
    process_axis_credit_cards()
    print("Done")


if __name__ == '__main__':
    main()
