#!/home/rabin/ipv-scraper/.venv/bin/python

# Run it once in a while but not twice in the same day

import csv
import os.path
from datetime import date
from glob import glob

from twarc import Twarc
from twarc.json2csv import get_headings, get_row

file_names = glob(os.path.join("splitted_keywords", "keywords_*.txt"))


today = date.today().isoformat()

save_file = os.path.join("past_tweets", f"tweets_{today}.csv")

if os.path.isfile(save_file):
    print(f"{save_file} already exists, skipping")
    exit(1)

header = None

t = Twarc()
with open(save_file, "w") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(get_headings())

    for file_name in file_names:
        with open(file_name) as input_file:
            query = input_file.read()

        for status in t.search(query, lang="ne"):
            csv_writer.writerow(get_row(status))
