#!/bin/bash

# File to read counter from
counter_file=counter.txt

# File counter
counter=$(< $counter_file)

# Twitter data base dir
twitter_data_dir=realtime_tweets

### Check if a directory does not exist ###
if [ ! -d $twitter_data_dir ]
then
    echo "Directory $twitter_data_dir DOES NOT exists. Creating the directory."
    mkdir -p $twitter_data_dir
fi

# Keywords separated by commas
keywords=$(paste -sd, keywords.txt)

### Write counter to file before running the script ###
echo $((counter + 1)) > $counter_file

# Run twarc from the environment
~/ipv-scraper/.venv/bin/twarc filter "$keywords" --lang ne --output $twitter_data_dir/realtime_$counter.csv --split 500 --format csv & disown
