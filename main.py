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


def parse_hdfc_bank_transaction_line(row):
    if float(row['DEBIT']) > 0:
        return row['DEBIT'], 'DEBIT'
    elif float(row['CREDIT']) > 0:
        return row['CREDIT'], 'CREDIT'
    else:
        raise Exception('unexpected row')


def process_hdfc_bank():
    hdfc_bank_path = 'data/hdfc_bank'
    cols = ['DATE', 'DESCRIPTION', 'VALUE_DATE', 'DEBIT', 'CREDIT', 'CHQ', 'BALANCE']
    date_pattern = re.compile(r'^\d{2}/\d{2}/\d{2}$')  # Regex pattern to match 'dd-mm-yyyy'

    data = []
    for root, dirs, files in os.walk(hdfc_bank_path):
        for filename in files:
            if filename.endswith('.txt'):
                file_path = os.path.join(root, filename)
                print(f"Processing file: {file_path}")
                with open(file_path, 'r') as f:
                    for line in f:
                        fl = line.strip()
                        formatted_line = [l.strip() for l in fl.split(',')]
                        if len(formatted_line) == len(cols):
                            data.append(formatted_line)

    combined_df = pd.DataFrame(data, columns=cols)
    combined_df = combined_df[combined_df.iloc[:, 0].apply(lambda x: bool(date_pattern.match(str(x))))]

    combined_df[['AMOUNT', 'TRANSACTION_TYPE']] = combined_df.apply(parse_hdfc_bank_transaction_line, axis=1, result_type='expand')
    formatted_df = combined_df[['DATE', 'DESCRIPTION', 'AMOUNT', 'TRANSACTION_TYPE']].copy()
    formatted_df['DATE'] = formatted_df['DATE'].apply(lambda x: x.replace('/', '-'))

    formatted_df.columns = [col.lower() for col in formatted_df.columns]
    formatted_df['credit_cards_name'] = hdfc_bank_path.split('/')[-1]
    formatted_df.to_csv('processed_data/hdfc_bank_transactions.csv', index=False)
    print("Combined DataFrame saved to 'hdfc_bank_transactions.csv'")


def parse_hdfc_credit_cards_transaction_line(line):
    print(line)
    date_pattern = r'^\d{2}/\d{2}/\d{4}'
    match = re.match(date_pattern, line)
    if not match:
        return None

    line = line.replace(' HDFC BANK UPI RuPay Credit Card Statement', '')
    line = line.replace(' Diners Club International Credit Card Statement', '')

    parts = line.split()
    if len(parts) < 4:
        return None

    date = parts[0].replace('/', '-')
    amount = parts[-2] if parts[-1] == 'Cr' else parts[-1]
    transaction_type = 'CREDIT' if parts[-1] == 'Cr' else 'DEBIT'
    description = ' '.join(parts[1:-2]) if transaction_type == 'CREDIT' else ' '.join(parts[1:-1])

    pattern = r'^\d{2}:\d{2}:\d{2}\s*'
    cleaned_description = re.sub(pattern, '', description)

    try:
        return date, cleaned_description, abs(float(amount.replace(',', ''))), transaction_type
    except Exception as e:
        print(amount.replace(',', ''))
        raise e


def process_hdfc_credit_cards():
    hdfc_credit_cards = ['data/hdfc_rupay', 'data/hdfc_diners']
    combined_df = pd.DataFrame()
    for card in hdfc_credit_cards:
        hdfc_data = []
        content = extract_text_from_pdfs(directory=card, password=None, page_number=None)
        for file_name, lines in content.items():
            for line in lines:
                print(file_name)
                parsed_line = parse_hdfc_credit_cards_transaction_line(line.strip())
                if parsed_line:
                    date, description, amount, transaction_type = parsed_line
                    hdfc_data.append({
                        "date": date,
                        "description": description,
                        "amount": abs(amount),
                        "transaction_type": transaction_type
                    })
                    print(len(hdfc_data))
        df = pd.DataFrame(hdfc_data)
        df['credit_cards_name'] = card.split('/')[-1]
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    combined_df.columns = [col.lower() for col in combined_df.columns]
    combined_df.to_csv('processed_data/hdfc_credit_cards_transactions.csv', index=False)


def process_amex():
    amex_path = 'data/amex'
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame to store combined data
    for root, dirs, files in os.walk(amex_path):
        for filename in files:
            if filename.endswith('.csv'):
                file_path = os.path.join(root, filename)
                print(f"Processing file: {file_path}")
                df = pd.read_csv(file_path)
                print(df.columns)
                df = df[['Date', 'Description', 'Amount']]
                df.columns = ['DATE', 'DESCRIPTION', 'AMOUNT']
                df['TRANSACTION_TYPE'] = df['AMOUNT'].apply(lambda x: 'DEBIT' if x >= 0 else 'CREDIT')
                df['DATE'] = df['DATE'].apply(lambda x: x.replace('/', '-'))
                df['CREDIT_CARDS_NAME'] = amex_path.split('/')[-1]
                combined_df = pd.concat([combined_df, df], ignore_index=True)
    combined_df.columns = [col.lower() for col in combined_df.columns]
    combined_df.to_csv('processed_data/amex_transactions.csv', index=False)


def process_onecard():
    onecard_path = 'data/onecard/manual_processed'
    one_card_data = []
    for root, dirs, files in os.walk(onecard_path):
        for filename in files:
            if filename.endswith('.txt'):
                file_path = os.path.join(root, filename)
                print(f"Processing file: {file_path}")
                with open(file_path, 'r') as f:
                    for line in f:
                        parts = line.strip().split(' ')
                        if len(parts) > 2:
                            amount = float(parts[-1].replace(',', ''))
                            one_card_data.append({
                                "DATE": datetime.strptime(parts[0], "%d-%b-%Y").strftime("%d-%m-%Y"),
                                "DESCRIPTION": ' '.join(parts[1:-2]),
                                "AMOUNT": abs(amount),
                                "TRANSACTION_TYPE": 'DEBIT' if amount >= 0 else 'CREDIT'
                            })

    df = pd.DataFrame(one_card_data)
    df['CREDIT_CARDS_NAME'] = onecard_path.split('/')[1]
    df.columns = [col.lower() for col in df.columns]
    df.to_csv('processed_data/onecard_transactions.csv', index=False)


def main():
    print('Started')
    process_idfc_wow()
    process_axis_bank()
    process_axis_credit_cards()
    process_hdfc_bank()
    process_hdfc_credit_cards()
    process_amex()
    process_onecard()
    print("Done")


if __name__ == '__main__':
    main()
