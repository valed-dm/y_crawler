import asyncio
import logging
import os
import time

import aiohttp
from bs4 import BeautifulSoup

from hn_scraper.constants import EXTERNAL_TIMEOUT
from hn_scraper.constants import MAX_CONTENT_SIZE
from hn_scraper.constants import REQUEST_DELAY
from hn_scraper.constants import SAVE_DIR
from hn_scraper.data_utils import sanitize_filename
from hn_scraper.data_utils import save_to_file

logger = logging.getLogger(__name__)

semaphore = asyncio.Semaphore(5)


async def fetch(session, url, timeout):
    async with semaphore:
        await asyncio.sleep(REQUEST_DELAY)
        try:
            start_time = time.time()
            async with session.get(url, timeout=timeout) as response:
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' not in content_type:
                    logger.warning(f"Skipping non-text content: {url}")
                    return None, 0, 0

                if response.content_length and response.content_length > MAX_CONTENT_SIZE:
                    logger.warning(f"Skipping {url} due to large content size ({response.content_length} bytes).")
                    return None, 0, 0

                content = await response.text()
                data_size = len(content.encode('utf-8'))
                fetch_time = time.time() - start_time
                return content, data_size, fetch_time
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Error fetching {url}: {e}")
            return None, 0, 0


async def fetch_comments(session, item_id, comments_url):
    try:
        comments_page, _, _ = await fetch(session, comments_url, EXTERNAL_TIMEOUT)
        if comments_page:
            comments_folder_path = os.path.join(SAVE_DIR, item_id, 'comments')
            os.makedirs(comments_folder_path, exist_ok=True)
            comments_file_path = os.path.join(comments_folder_path, 'comments.html')
            await save_to_file(comments_file_path, comments_page)

            soup = BeautifulSoup(comments_page, 'html.parser')
            comment_links = soup.select('a[href^="http"]')

            for link in comment_links:
                comment_url = link['href']
                sanitized_filename = sanitize_filename(link.get_text()) + '.html'
                comment_filename = os.path.join(comments_folder_path, sanitized_filename)
                try:
                    comment_page, _, _ = await fetch(session, comment_url, EXTERNAL_TIMEOUT)
                    if comment_page:
                        await save_to_file(comment_filename, comment_page)
                except UnicodeDecodeError as e:
                    logger.error(f"UnicodeDecodeError in comment page: {e}")
                    # Handle the error appropriately (e.g., logging, skipping problematic content)
            return True
    except UnicodeDecodeError as e:
        logger.error(f"UnicodeDecodeError in comments_page: {e}")
        # Handle the error appropriately (e.g., logging, skipping problematic content)
    return False


async def load_comments_for_all_items(session, items):
    for item in items:
        item_id, folder_name = item
        comments_url = f'https://news.ycombinator.com/item?id={item_id}'
        success = await fetch_comments(session, item_id, comments_url)
        if not success:
            logger.error(f"Failed to load comments for item ID: {item_id}")
        await asyncio.sleep(REQUEST_DELAY)
