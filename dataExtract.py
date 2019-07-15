#!/usr/bin/env python
# coding: utf-8

import sys
import time
import datetime as dt
import pandas as pd
import json
import re
import os
import psutil
from pathlib import Path


if (len(sys.argv) < 2):
    print("Please provide a valid .csv filename")
else:
    filename = sys.argv[1]
    pattern = re.compile("^[a-zA-Z0-9_-]+(\.csv)$")
    matches = bool(pattern.match(filename))
    if (matches == False):
        print("Please provide a valid .csv filename")
    else:
        print("Getting data...")
        title = []  #citation title
		abstract = [] #abstract
        first_seen = [] #citation first seen on timestamp
        publish_date = [] #publish date for article
        Subjects = [] #scopus subjects
        Journal = [] #journal names
        
        directory_in_str = "D:\\chrome-dowload\\almetrics\\keys1"
        pathlist = list(Path(directory_in_str).glob('**/*.txt'))
        try:
            file_limit = int(sys.argv[2])
        except:
            file_limit = len(pathlist)
        start_time = time.perf_counter()
        for counter, path in enumerate(pathlist):
            if (counter > file_limit):
                break
            percent = (counter + 1) / file_limit * 100
            elapsed_time = int(time.perf_counter() - start_time)
            elapsed_str = str(dt.timedelta(seconds=elapsed_time))
            remaining_time = int((100 - percent) / percent * elapsed_time)
            remaining_str = str(dt.timedelta(seconds=remaining_time))
            process = psutil.Process(os.getpid())
            memory = process.memory_info().rss / 1024 / 1024
            output = "Processing file {} of {}, {:.1f}% complete, {} elapsed, {} remaining, using {:.2f}MB"
            output_built = output.format(counter, file_limit, percent, elapsed_str, remaining_str, memory)
            print(output_built, end='\r')
            path_in_str = str(path)
            
            with open(path_in_str) as f:
                for line in f:
                    record = json.loads(line)
                    
                    try:
                        title.append(record['citation']['title'])
                    except:
                        title.append(0)
						
					try:
                        abstract.append(record['citation']['abstract'])
                    except:
                        abstract.append(0)
						
                    try:
                        first_seen.append(record['citation']['first_seen_on'])
                    except:
                        first_seen.append(0)

                    try:
                        publish_date.append(record['citation']['pubdate'])
                    except:
                        publish_date.append(0)

                    try:
                        Subjects.append(record['citation']['scopus_subjects'])
                    except:
                        Subjects.append(0)

                    try:
                        Journal.append(record['citation']['journal'])
                    except:
                        Journal.append(0)

                    
        print("\nPackaging data...")
        
        data = {
            'Title' : pd.Series(title),
            'Firstseen' : pd.Series(first_seen),
            'Publish_date' : pd.Series(publish_date),
            'Scopus_Subjects' : pd.Series(Subjects),
            'Journal_Names' : pd.Series(Journal)
        }

        df = pd.DataFrame(data)
        size = "Created DataFrame with {} records"
        print(size.format(len(df.index)))
        print("Writing data to file...")
        df.to_csv(filename)






