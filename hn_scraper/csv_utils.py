import asyncio
import csv
import os
from io import StringIO

import aiofiles

from hn_scraper.constants import CSV_FILE_PATH

csv_lock = asyncio.Lock()


async def load_csv():
    async with csv_lock:
        if os.path.exists(CSV_FILE_PATH):
            async with aiofiles.open(CSV_FILE_PATH, 'r', newline='') as csv_file:
                lines = await csv_file.readlines()
                rows = list(csv.reader(lines))
                if rows:
                    return rows[0], rows[1:]
        return ['position', 'item_id', 'title', 'downloaded_at', 'folder_name'], []


async def save_csv(header, rows):
    rows.sort(key=lambda x: int(x[0]))
    async with csv_lock:
        output = StringIO()
        csv_writer = csv.writer(output)
        csv_writer.writerow(header)
        csv_writer.writerows(rows)
        csv_content = output.getvalue()

        async with aiofiles.open(CSV_FILE_PATH, 'w', newline='') as csv_file:
            await csv_file.write(csv_content)


async def update_csv_positions_async(filepath, items_order):
    async with aiofiles.open(filepath, 'r', newline='') as csv_file:
        lines = await csv_file.readlines()
        rows = list(csv.reader(lines))

    item_id_to_position = {item_id: i + 1 for i, item_id in enumerate(items_order)}

    for row in rows[1:]:
        item_id = row[1]
        if item_id in item_id_to_position:
            row[0] = str(item_id_to_position[item_id])

    rows[1:] = sorted(rows[1:], key=lambda x: int(x[0]))

    output = StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerows(rows)
    csv_content = output.getvalue()

    async with aiofiles.open(filepath, 'w', newline='') as csv_file:
        await csv_file.write(csv_content)
