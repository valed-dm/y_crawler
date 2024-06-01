import asyncio
import csv
import logging
import os
import time

import aiohttp
from bs4 import BeautifulSoup

from hn_scraper.constants import CSV_FILE_PATH
from hn_scraper.constants import HACKER_NEWS_URL
from hn_scraper.constants import MAIN_PAGE_TIMEOUT
from hn_scraper.constants import POLL_INTERVAL
from hn_scraper.constants import SAVE_DIR
from hn_scraper.csv_utils import load_csv
from hn_scraper.csv_utils import save_csv
from hn_scraper.csv_utils import update_csv_positions_async
from hn_scraper.data_utils import clear_old_data
from hn_scraper.fetch_utils import fetch
from hn_scraper.fetch_utils import load_comments_for_all_items
from hn_scraper.parse_utils import parse_item

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler("hn_scraper.log"),
    logging.StreamHandler()
])


# Concurrency control
csv_lock = asyncio.Lock()
semaphore = asyncio.Semaphore(5)

# Ensure directories and files exist
os.makedirs(SAVE_DIR, exist_ok=True)
if not os.path.exists(CSV_FILE_PATH):
    with open(CSV_FILE_PATH, 'w', newline='') as csv_file:
        csv.writer(csv_file).writerow(['position', 'item_id', 'title', 'downloaded_at', 'folder_name'])


async def main():
    while True:
        poll_start_time = time.time()

        async with aiohttp.ClientSession() as session:
            tasks = []
            items_order = []
            items_for_comments = []

            main_page, _, _ = await fetch(session, HACKER_NEWS_URL, MAIN_PAGE_TIMEOUT)
            if not main_page:
                logging.error("Failed to fetch the main Hacker News page.")
                await asyncio.sleep(POLL_INTERVAL)
                continue

            soup = BeautifulSoup(main_page, 'html.parser')
            items = soup.select('tr.athing')

            header, csv_rows = await load_csv()

            origin_ids = {item.get('id') for item in items}
            local_ids = {row[1] for row in csv_rows if len(row) >= 2}
            local_remove_ids = local_ids - origin_ids

            for remove_item_id in local_remove_ids:
                clear_old_data(remove_item_id)
                csv_rows = [row for row in csv_rows if len(row) >= 2 and row[1] != remove_item_id]
                logging.info(f"Removed old data for item ID: {remove_item_id}")

            await save_csv(header, csv_rows)

            for position, item in enumerate(items[:31], start=1):
                item_id = item.get('id')
                items_order.append(item_id)

                if not item_id:
                    logging.warning(f"Skipping item due to missing ID. HTML: {item}")
                    continue

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
                    items_for_comments.append((item_id, folder_name))

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

            await load_comments_for_all_items(session, items_for_comments)

        await asyncio.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    asyncio.run(main())
