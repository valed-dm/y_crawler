import logging
import os
from datetime import datetime

from hn_scraper.constants import EXTERNAL_TIMEOUT
from hn_scraper.constants import SAVE_DIR
from hn_scraper.csv_utils import load_csv
from hn_scraper.csv_utils import save_csv
from hn_scraper.data_utils import save_to_file
from hn_scraper.fetch_utils import fetch

logger = logging.getLogger(__name__)


async def parse_item(session, position, item_link, item_id, item_title, folder_name):
    external_page, data_size, fetch_time = await fetch(session, item_link, EXTERNAL_TIMEOUT)

    if external_page:
        external_file_path = os.path.join(SAVE_DIR, folder_name, f'{item_id}_external.html')
        await save_to_file(external_file_path, external_page)

        downloaded_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        header, csv_rows = await load_csv()

        existing_row = next((row for row in csv_rows if row[1] == item_id), None)
        if existing_row:
            return False, data_size, fetch_time

        csv_rows.append([position, item_id, item_title, downloaded_at, folder_name])
        await save_csv(header, csv_rows)
        logger.info(f"Loaded item ID: {item_id}, Title: {item_title}")

        return True, data_size, fetch_time

    header, csv_rows = await load_csv()
    csv_rows.append([position, item_id, item_title, 'download failure', folder_name])
    await save_csv(header, csv_rows)
    return False, 0, 0
