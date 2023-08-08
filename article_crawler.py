import multiprocessing
import re
import requests
import justext
import random
import os

from multiprocessing import Manager
from itertools import repeat

# CONFIG
TXT_FILES_PATH = 'your-directory-path/'  # remember about the '/', has to be created before starting the script
TXT_URLS = 'txt-file-with-urls'  # provide directory to .txt containing URLs
MIN_LENGTH = 2000  # minimal number of characters for a text to be collected
PROCESSES = 3  # number of processes, from 1 to os.cpu_count()


###

def process_item(item, counter, total_files):
    counter.value += 1
    ccc = random.randrange(1, 1000000000)
    txt = ''
    response = requests.get(item)

    """
    Tool used to analyze and extract text from websites is a library called 'JusText' by Jan Pomikalek. 

    Be sure to experiment with different URLs from set manually and fine-tune the params provided by the author. 
    Ones provided below are the results of experiments on a set of URLs from a forum type website. Most likely they
    will not fit to your use-case.

    Fine-tuning should be done once per website/dataset.

    To try different possibilites and sets of params, visit:
    https://nlp.fi.muni.cz/projects/justext/
    """

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
    print("Processing file: " + item, counter.value, "/", total_files, round((counter.value / total_files * 100), 2),
          "%")

    if len(txt) > MIN_LENGTH:
        try:
            with open(f"{TXT_FILES_PATH}" + str(ccc) + ".txt", 'w', encoding="utf-8") as f:
                f.write(txt)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    manager = Manager()
    counter = manager.Value('i', 0)

    with open(TXT_URLS, "r", encoding="utf-8") as f:
        txt_files = f.read().split("\n")

    total_files = len(txt_files)

    with multiprocessing.Pool(processes=PROCESSES) as pool:
        results = pool.starmap(process_item,
                               zip(txt_files,
                                   repeat(counter),
                                   repeat(total_files)),
                               chunksize=8)