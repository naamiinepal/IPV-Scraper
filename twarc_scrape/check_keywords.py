from unicodedata import normalize

keywords_file = "keywords.txt"


def normalizer(line: str):
    return normalize("NFC", line.strip())


with open(keywords_file) as f:
    raw_file = f.read()

keywords = tuple(map(normalizer, raw_file.splitlines()))

keyword_set = set(keywords)
if len(keywords) > len(keyword_set):
    print("Duplicate keywords. Writing unique ones to the file")

    # To make the changes in the file less
    sorted_keywords = "\n".join(
        sorted(keyword_set, key=lambda line: line.strip("'").split(" ")[0])
    )
    with open(keywords_file, "w") as f:
        f.write(sorted_keywords)
    exit(1)
else:
    print("The keywords are unique")
