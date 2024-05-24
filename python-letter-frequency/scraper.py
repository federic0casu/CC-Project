import os
import requests
from bs4 import BeautifulSoup

# Define the base URL for Project Gutenberg and the directory to save the files
base_url = "https://www.gutenberg.org"
save_dir = "../data/books/italian/"

# Ensure the save directory exists
os.makedirs(save_dir, exist_ok=True)

def get_ebook_links(main_url):

    headers = {
        'Content-Type': 'multipart/form-data; boundary=----WebKitFormBoundaryq3dYOGxyEsUjkFGj'
    }
    body = (
        "------WebKitFormBoundaryq3dYOGxyEsUjkFGj\r\n"
        "Content-Disposition: form-data; name=\"author\"\r\n\r\n"
        "a\r\n"
        "------WebKitFormBoundaryq3dYOGxyEsUjkFGj\r\n"
        "Content-Disposition: form-data; name=\"title\"\r\n\r\naca\r\n"
        "------WebKitFormBoundaryq3dYOGxyEsUjkFGj\r\n"
        "Content-Disposition: form-data; name=\"subject\"\r\n\r\n\r\n"
        "------WebKitFormBoundaryq3dYOGxyEsUjkFGj\r\n"
        "Content-Disposition: form-data; name=\"lang\"\r\n\r\nen\r\n"
        "------WebKitFormBoundaryq3dYOGxyEsUjkFGj\r\n"
        "Content-Disposition: form-data; name=\"category\"\r\n\r\n\r\n"
        "------WebKitFormBoundaryq3dYOGxyEsUjkFGj\r\n"
        "Content-Disposition: form-data; name=\"locc\"\r\n\r\n\r\n"
        "------WebKitFormBoundaryq3dYOGxyEsUjkFGj\r\n"
        "Content-Disposition: form-data; name=\"filetype\"\r\n\r\ntxt.utf-8\r\n"
        "------WebKitFormBoundaryq3dYOGxyEsUjkFGj\r\n"
        "Content-Disposition: form-data; name=\"submit_search\"\r\n\r\nSearch\r\n"
        "------WebKitFormBoundaryq3dYOGxyEsUjkFGj--\r\n"
    )
    """Get all hrefs that start with '/ebooks/' from the main URL."""

    response = requests.post(main_url, headers=headers, data=body)
    response.raise_for_status()  # Ensure we notice bad responses

    soup = BeautifulSoup(response.text, 'html.parser')
    ebook_links = []

    # Find all links that start with '/ebooks/'
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.startswith('/ebooks/'):
            ebook_links.append(href)
            print(href)
    
    return ebook_links

def download_ebook(content_id):
    """Download the ebook content and save it to the specified directory."""
    download_url = f"https://www.gutenberg.org/cache/epub/{content_id}/pg{content_id}.txt"
    response = requests.get(download_url)
    
    if response.status_code == 200:
        # Save the content to a file
        file_path = os.path.join(save_dir, f"{content_id}.txt")
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(response.text)
        print(f"Downloaded and saved: {file_path}")
    else:
        print(f"Failed to download: {download_url}")

def main():
    main_url = "https://www.gutenberg.org/ebooks/results/"
    ebook_links = get_ebook_links(main_url)

    # Extract the content ID and download the ebook content
    for link in ebook_links:
        content_id = link.split('/')[2]
        download_ebook(content_id)

if __name__ == "__main__":
    main()
