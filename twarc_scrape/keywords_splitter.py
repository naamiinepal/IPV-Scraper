import os

with open("keywords.txt") as f:
    raw_file = f.read()

keywords = tuple(line.strip("'") for line in raw_file.splitlines())


dir_name = "splitted_keywords"

os.makedirs(dir_name, exist_ok=True)

step = 10  # Recommended step size for keywords search

for i in range(0, len(keywords), step):
    with open(os.path.join(dir_name, f"keywords_{i}.txt"), "w") as f:
        f.write(" OR ".join(keywords[i : i + step]))
