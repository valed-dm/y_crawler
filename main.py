import csv
import aiohttp
import aiofiles
import asyncio
import os
from bs4 import BeautifulSoup
from datetime import datetime
import time
import logging
from io import StringIO

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("hn_scraper.log"),
                              logging.StreamHandler()])

# URL of Hacker News
HACKER_NEWS_URL = 'https://news.ycombinator.com/'

# Time interval to check for new items (in seconds)
POLL_INTERVAL = 60  # e.g., 1 minute

# Directory to save the data
SAVE_DIR = 'hn_data'

# CSV file path
CSV_FILE_PATH = os.path.join(SAVE_DIR, 'downloaded_items.csv')

# Ensure the save directory exists
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)

# Ensure the CSV file exists and has headers
if not os.path.exists(CSV_FILE_PATH):
    with open(CSV_FILE_PATH, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['position', 'item_id', 'title', 'downloaded_at', 'folder_name'])

# Limit on the size of content to be fetched (in bytes)
MAX_CONTENT_SIZE = 10 * 1024 * 1024  # 10 MB

csv_lock = asyncio.Lock()

# Timeouts
MAIN_PAGE_TIMEOUT = 10  # seconds for main page
EXTERNAL_TIMEOUT = 10  # seconds for external URLs


async def fetch(session, url, timeout):
    """Fetch the content of the URL using the provided session."""
    try:
        start_time = time.time()
        async with session.get(url, timeout=timeout) as response:
            if response.content_length and response.content_length > MAX_CONTENT_SIZE:
                logging.warning(f"Skipping {url} due to large content size ({response.content_length} bytes).")
                return None, 0, 0

            content = await response.text()
            data_size = len(content.encode('utf-8'))  # size in bytes
            fetch_time = time.time() - start_time
            return content, data_size, fetch_time
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching {url}: {e}")
        return None, 0, 0
    except asyncio.TimeoutError:
        logging.error(f"Timeout fetching {url}")
        return None, 0, 0
    except Exception as e:
        logging.error(f"Unexpected error fetching {url}: {e}")
        return None, 0, 0


async def save_to_file(filepath, content):
    """Save the given content to the specified file."""
    try:
        async with aiofiles.open(filepath, 'w') as file:
            await file.write(content)
        logging.info(f"Successfully saved content to {filepath}")
    except Exception as e:
        logging.error(f"Error saving to {filepath}: {e}")


async def load_csv():
    """Load the CSV file and return its content as a list of rows, separating the header."""
    async with csv_lock:
        if os.path.exists(CSV_FILE_PATH):
            async with aiofiles.open(CSV_FILE_PATH, 'r', newline='') as csv_file:
                lines = await csv_file.readlines()
                csv_reader = csv.reader(lines)
                rows = list(csv_reader)
                if rows:
                    header = rows[0]
                    data = rows[1:]
                    return header, data
        return ['position', 'item_id', 'title', 'downloaded_at', 'folder_name'], []


async def save_csv(header, rows):
    """Save the given list of rows to the CSV file, sorted by position."""
    rows.sort(key=lambda x: int(x[0]))  # Sort by position
    async with csv_lock:
        output = StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerow(header)
        csv_writer.writerows(rows)
        csv_content = output.getvalue()

        async with aiofiles.open(CSV_FILE_PATH, 'w', newline='') as csv_file:
            await csv_file.write(csv_content)


async def update_csv_positions_async(filepath, items_order):
    # Load CSV data asynchronously
    async with aiofiles.open(filepath, 'r', newline='') as csv_file:
        lines = await csv_file.readlines()
        csv_reader = csv.reader(lines)
        rows = list(csv_reader)

    # Create a dictionary to map item_id to its position in the items_order list
    item_id_to_position = {item_id: i + 1 for i, item_id in enumerate(items_order)}

    # Update the position in CSV rows based on the item_id_to_position dictionary
    for row in rows[1:]:  # Skip header row
        item_id = row[1]
        if item_id in item_id_to_position:
            row[0] = str(item_id_to_position[item_id])

    # Sort rows by the new positions
    rows[1:] = sorted(rows[1:], key=lambda x: int(x[0]))

    # Build CSV content in memory
    output = StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerows(rows)
    csv_content = output.getvalue()

    # Write the updated CSV data back to the file asynchronously
    async with aiofiles.open(filepath, 'w', newline='') as csv_file:
        await csv_file.write(csv_content)


def clear_old_data(folder_name):
    """Remove the directory and files associated with the given folder name."""
    dirpath = os.path.join(SAVE_DIR, folder_name)
    if os.path.exists(dirpath):
        for filename in os.listdir(dirpath):
            os.remove(os.path.join(dirpath, filename))
        os.rmdir(dirpath)
        logging.info(f"Removed old data for folder: {folder_name}")


async def parse_item(session, position, item_link, item_id, item_title, folder_name):
    """Fetch and save the content of the external link, and update CSV."""
    external_page, data_size, fetch_time = await fetch(session, item_link, EXTERNAL_TIMEOUT)

    if external_page:
        external_file_path = os.path.join(SAVE_DIR, folder_name, f'{item_id}_external.html')
        await save_to_file(external_file_path, external_page)

        # Log the item to CSV
        downloaded_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        header, csv_rows = await load_csv()

        # Check if item already exists in CSV to prevent duplicate entries
        existing_row = next((row for row in csv_rows if row[1] == item_id), None)
        if existing_row:
            return False, data_size, fetch_time  # Return a flag indicating not loaded

        csv_rows.append([position, item_id, item_title, downloaded_at, folder_name])
        await save_csv(header, csv_rows)
        logging.info(f"Loaded item ID: {item_id}, Title: {item_title}")
        return True, data_size, fetch_time  # Return a flag indicating loaded

    header, csv_rows = await load_csv()
    csv_rows.append([position, item_id, item_title, 'download failure', folder_name])
    await save_csv(header, csv_rows)
    return False, 0, 0  # Return a flag indicating not loaded


async def main():
    while True:
        poll_start_time = time.time()
        async with aiohttp.ClientSession() as session:
            main_page, _, _ = await fetch(session, HACKER_NEWS_URL, MAIN_PAGE_TIMEOUT)
            if not main_page:
                logging.warning("Failed to fetch the main page, retrying after the poll interval...")
                await asyncio.sleep(POLL_INTERVAL)
                continue

            tasks = []
            items_order = []

            # Find all items on the main page
            soup = BeautifulSoup(main_page, 'html.parser')
            items = soup.select('tr.athing')

            header, csv_rows = await load_csv()

            origin_ids = {item.get('id') for item in items}  # remote ids set
            local_ids = {row[1] for row in csv_rows}  # local ids set
            local_remove_ids = local_ids - origin_ids

            # Clear old local data not present in the current list
            for remove_item_id in local_remove_ids:
                clear_old_data(remove_item_id)
                csv_rows = [row for row in csv_rows if row[1] != remove_item_id]
                logging.info(f"Removed old data for item ID: {remove_item_id}")

            await save_csv(header, csv_rows)  # Save after clearing old data

            for position, item in enumerate(items[:31], start=1):
                item_id = item.get('id')
                items_order.append(item_id)

                if not item_id:
                    logging.warning(f"Skipping item due to missing ID. HTML: {item}")
                    continue

                # Find the title element
                title_element = item.select_one('td.title > span.titleline > a')
                if not title_element:
                    logging.warning(
                        f"Skipping item due to missing title element. ID: {item_id}. HTML: {item.prettify()}")
                    continue

                item_title = title_element.text
                item_link = title_element['href']
                if not item_link.startswith('http'):
                    item_link = f'https://news.ycombinator.com/{item_link}'

                logging.info(f"Processing item ID: {item_id}, Title: {item_title}, External URL: {item_link}")

                folder_name = str(item_id)
                item_save_path = os.path.join(SAVE_DIR, folder_name)
                os.makedirs(item_save_path, exist_ok=True)

                if item_id not in local_ids:
                    tasks.append(parse_item(session, position, item_link, item_id, item_title, folder_name))

            results = await asyncio.gather(*tasks)

            await update_csv_positions_async(CSV_FILE_PATH, items_order)

            successful_loads = sum(1 for success, _, _ in results if success)
            total_data_size = sum(data_size for _, data_size, _ in results)
            estimated_total_time = sum(fetch_time for _, _, fetch_time in results)

            end_poll_time = time.time()
            total_poll_time = end_poll_time - poll_start_time

            logging.info(f"Successful loads: {successful_loads}")
            logging.info(f"Total data size: {total_data_size} bytes")
            logging.info(f"Estimated total fetch time: {estimated_total_time:.2f} seconds")
            logging.info(f"Total poll time: {total_poll_time:.2f} seconds")

        await asyncio.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    asyncio.run(main())
