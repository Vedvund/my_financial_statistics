import os

import PyPDF2


def extract_text_from_pdfs(directory, password, page_number):
    # List all files in the specified directory
    for filename in os.listdir(directory):
        if filename.endswith('.pdf'):
            file_path = os.path.join(directory, filename)
            print(f"Processing file: {file_path}")

            # Open the PDF file
            with open(file_path, 'rb') as file:
                # Initialize the PDF reader
                reader = PyPDF2.PdfReader(file)

                # Check if the PDF is encrypted and try to decrypt it
                if reader.is_encrypted:
                    reader.decrypt(password)

                # Verify the page number is valid
                if page_number < len(reader.pages):
                    # Extract text from the specified page
                    page = reader.pages[page_number]
                    text = page.extract_text()

                    # Print the text line by line
                    if text:
                        return text.splitlines()
                    else:
                        print("No text found on the specified page.")
                else:
                    print(f"Page number {page_number} is out of range for this document.")
                return []
