import os
import re
import random
import requests
import justext
import multiprocessing
from multiprocessing import Manager
from itertools import repeat

# CONFIG
TXT_FILES_PATH = 'your-directory-path/'
TXT_URLS = 'txt-file-with-urls'
MIN_LENGTH = 2000
PROCESSES = 3
###


def process_item(item, file_count, file_total):
    txt = ''
    response = requests.get(item)
    paragraphs = justext.justext(response.content, justext.get_stoplist("Polish"),
                                 max_heading_distance=150,
                                 length_low=10,
                                 length_high=100,
                                 stopwords_low=0.1,
                                 stopwords_high=0.2,
                                 max_link_density=0.2)

    for paragraph in paragraphs:
        if not paragraph.is_boilerplate:
            txt += paragraph.text + " "

    txt = re.sub(r"\n{2,}", "\n", txt)

    with file_count.get_lock():
        file_count.value += 1
        print("Processing file: {}, {}/{}, {:.2f}%".format(item, file_count.value, file_total,
                                                           (file_count.value / file_total * 100)))

    if len(txt) > MIN_LENGTH:
        try:
            with open(os.path.join(TXT_FILES_PATH, f"{random.randint(1, 1_000_000_000)}.txt"), 'w',
                      encoding="utf-8") as file_handle:
                file_handle.write(txt)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    manager = Manager()
    counter = manager.Value('i', 0)

    with open(TXT_URLS, "r", encoding="utf-8") as f:
        txt_files = f.read().split("\n")

    total_files = len(txt_files)

    with multiprocessing.Pool(processes=PROCESSES) as pool:
        pool.starmap(process_item, zip(txt_files, repeat(counter), repeat(total_files)), chunksize=8)
