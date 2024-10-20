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
    formatted_df['DATE'] = formatted_df['DATE'].apply(lambda x: datetime.strptime(x, "%d-%m-%y").strftime("%d-%m-%Y"))

    formatted_df.columns = [col.lower() for col in formatted_df.columns]
    formatted_df['credit_cards_name'] = hdfc_bank_path.split('/')[-1]
    formatted_df.to_csv('processed_data/hdfc_bank_transactions.csv', index=False)


def parse_hdfc_credit_cards_transaction_line(line):
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
        raise e


def process_hdfc_credit_cards():
    hdfc_credit_cards = ['data/hdfc_rupay', 'data/hdfc_diners']
    combined_df = pd.DataFrame()
    for card in hdfc_credit_cards:
        hdfc_data = []
        content = extract_text_from_pdfs(directory=card, password=None, page_number=None)
        for file_name, lines in content.items():
            for line in lines:
                parsed_line = parse_hdfc_credit_cards_transaction_line(line.strip())
                if parsed_line:
                    date, description, amount, transaction_type = parsed_line
                    hdfc_data.append({
                        "date": date,
                        "description": description,
                        "amount": abs(amount),
                        "transaction_type": transaction_type
                    })
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
                df = pd.read_csv(file_path)
                df = df[['Date', 'Description', 'Amount']]
                df.columns = ['DATE', 'DESCRIPTION', 'AMOUNT']
                df['TRANSACTION_TYPE'] = df['AMOUNT'].apply(lambda x: 'DEBIT' if x >= 0 else 'CREDIT')
                df['DATE'] = df['DATE'].apply(lambda x: x.replace('/', '-'))
                df['CREDIT_CARDS_NAME'] = amex_path.split('/')[-1]
                df['AMOUNT'] = df['AMOUNT'].apply(lambda x: abs(x))
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


def parse_jupiter_transaction_line(line, date_str):
    formated_line = line.replace('Rs. ', '')
    parts = formated_line.split(' ')
    description = (' '.join(parts[1:-1]))[2:]
    amount = float(parts[-1].replace(',', ''))
    for key_word in ['REFUND', 'Repayment - Thank You']:
        if key_word in formated_line:
            transaction_type = 'CREDIT'
            break
        else:
            transaction_type = 'DEBIT'

    date = datetime.strptime(date_str.strip(), "%d %b %Y").strftime("%d-%m-%Y")

    return date, description, amount, transaction_type


def process_jupiter():
    jupiter_path = 'data/jupiter'
    jupiter_pdf_password = os.getenv('JUPITER_PDF_PASSWORD')
    content = extract_text_from_pdfs(directory=jupiter_path, password=jupiter_pdf_password, page_number=None)

    jupiter_data = []
    for file_name, lines in content.items():
        for index, line in enumerate(lines):
            time_pattern = r'^\d{2}:\d{2}'
            match = re.match(time_pattern, line)
            if not match:
                continue

            parsed_line = parse_jupiter_transaction_line(line, lines[index - 1])
            if parsed_line:
                date, description, amount, transaction_type = parsed_line
                jupiter_data.append({
                    "date": date,
                    "description": description,
                    "amount": abs(amount),
                    "transaction_type": transaction_type
                })

    jupiter_data_df = pd.DataFrame(jupiter_data)
    jupiter_data_df['credit_cards_name'] = jupiter_path.split('/')[1]
    jupiter_data_df.to_csv('processed_data/jupiter_transactions.csv', index=False)


def process_icici_amazon():
    icici_amazon_path = 'data/icici_amazon'
    date_pattern = r'^\d{2}/\d{2}/\d{4}'
    data = []
    for root, dirs, files in os.walk(icici_amazon_path):
        for filename in files:
            if filename.endswith('.csv'):
                file_path = os.path.join(root, filename)
                with open(file_path, 'r') as f:
                    for line in f:
                        fl = line.replace('"', '')

                        fl = fl.strip()
                        match = re.match(date_pattern, fl)
                        if not match:
                            continue
                        parts = fl.split(',')
                        data.append({
                            "date": parts[0].replace('/', '-'),
                            "sr_no": parts[1],
                            "description": parts[2],
                            "reward_point": parts[3],
                            "intl_amount": parts[4],
                            "amount": parts[5],
                            "transaction_type": 'CREDIT' if parts[6] == 'CR' else 'DEBIT'
                        })

    df = pd.DataFrame(data)
    df['credit_cards_name'] = icici_amazon_path.split('/')[1]
    df = df[['date', 'description', 'amount', 'transaction_type', 'credit_cards_name']]
    df.to_csv('processed_data/icici_amazon.csv', index=False)


def process_sbi_bank():
    sbi_bank_path = 'data/sbi_bank/manual_processed'
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame to store combined data
    for root, dirs, files in os.walk(sbi_bank_path):
        for filename in files:
            if filename.endswith('.txt'):
                file_path = os.path.join(root, filename)
                df = pd.read_csv(file_path, sep='\t')
                df.columns = ['date', 'value_date', 'description', 'ref_no_cheque_no', 'debit', 'credit', 'balance']
                combined_df = pd.concat([combined_df, df], ignore_index=True)

    combined_df['debit'] = combined_df['debit'].apply(lambda x: float(x.replace(',', '')) if x != ' ' else None)
    combined_df['credit'] = combined_df['credit'].apply(lambda x: float(x.replace(',', '')) if x != ' ' else None)
    combined_df['amount'] = combined_df['debit'].fillna(combined_df['credit'])
    combined_df['transaction_type'] = combined_df['debit'].apply(lambda x: 'CREDIT' if pd.isna(x) else 'DEBIT')
    combined_df['date'] = combined_df['date'].apply(lambda x: datetime.strptime(x, "%d %b %Y").strftime("%d-%m-%Y"))
    combined_df['credit_cards_name'] = sbi_bank_path.split('/')[1]
    new_df = combined_df[['date', 'description', 'amount', 'transaction_type', 'credit_cards_name']]

    new_df.to_csv('processed_data/sbi_bank.csv', index=False)


def combine_all_processed():
    combined_df = pd.DataFrame()
    for root, dirs, files in os.walk('processed_data'):
        for filename in files:
            if filename.endswith('.csv') and 'all_accounts' not in filename:
                file_path = os.path.join(root, filename)
                df = pd.read_csv(file_path)
                combined_df = pd.concat([combined_df, df], ignore_index=True)
    combined_df.drop_duplicates(keep='first', inplace=True)
    combined_df.to_csv('processed_data/all_accounts.csv', index=False)


def main():
    process_idfc_wow()
    process_axis_bank()
    process_axis_credit_cards()
    process_hdfc_bank()
    process_hdfc_credit_cards()
    process_amex()
    process_onecard()
    process_jupiter()
    process_icici_amazon()
    process_sbi_bank()
    combine_all_processed()


if __name__ == '__main__':
    main()
