import os

import PyPDF2


def extract_text_from_pdfs(directory, password, page_number):
    content = {}

    # Use os.walk to recurse through directory and subdirectories
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.pdf') or filename.endswith('.PDF'):
                file_path = os.path.join(root, filename)
                # Open the PDF file
                with open(file_path, 'rb') as file:
                    # Initialize the PDF reader
                    reader = PyPDF2.PdfReader(file)

                    # Check if the PDF is encrypted and try to decrypt it
                    if reader.is_encrypted:
                        try:
                            reader.decrypt(password)
                        except Exception as e:
                            print(f"Failed to decrypt {file_path}: {e}")
                            continue

                    # Verify the page number is valid
                    if page_number and page_number < len(reader.pages):
                        # Extract text from the specified page
                        page = reader.pages[page_number]
                        text = page.extract_text()

                        # Save the text line by line in the content dictionary
                        if text:
                            content[file_path] = text.splitlines()
                        else:
                            print("No text found on the specified page.")
                    elif not page_number:
                        for i in range(len(reader.pages)):
                            page = reader.pages[i]
                            text = page.extract_text()

                            # Save the text line by line in the content dictionary
                            if text:
                                if file_path not in content:
                                    content[file_path] = text.splitlines()
                                else:
                                    content[file_path] += text.splitlines()
                    else:
                        print(f"Page number {page_number} is out of range for this document.")

    return content
