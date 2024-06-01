import os
import re


HACKER_NEWS_URL = 'https://news.ycombinator.com/'
POLL_INTERVAL = 600  # Time interval to check for new items (in seconds)
SAVE_DIR = 'hn_data'
CSV_FILE_PATH = os.path.join(SAVE_DIR, 'downloaded_items.csv')
MAX_CONTENT_SIZE = 10 * 1024 * 1024  # Limit on the size of content to be fetched (10 MB)
REQUEST_DELAY = .5  # seconds
MAIN_PAGE_TIMEOUT = 10  # seconds
EXTERNAL_TIMEOUT = 10  # seconds
INVALID_FILENAME_CHARS = re.compile(r'[:<>"/\\|?*]')
