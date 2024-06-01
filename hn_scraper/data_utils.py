import logging
import os
import shutil

import aiofiles

from hn_scraper.constants import INVALID_FILENAME_CHARS
from hn_scraper.constants import SAVE_DIR

logger = logging.getLogger(__name__)


def sanitize_filename(filename):
    return INVALID_FILENAME_CHARS.sub('_', filename)


async def save_to_file(filepath, content):
    try:
        async with aiofiles.open(filepath, 'w') as file:
            await file.write(content)
        logger.info(f"Successfully saved content to {filepath}")
    except Exception as e:
        logger.error(f"Error saving to {filepath}: {e}")


def clear_old_data(folder_name):
    dirpath = os.path.join(SAVE_DIR, folder_name)
    if os.path.exists(dirpath):
        shutil.rmtree(dirpath)
        logger.info(f"Removed old data for folder: {folder_name}")
