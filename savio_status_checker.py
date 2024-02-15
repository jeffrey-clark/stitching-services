from Models.Savio import SavioClient, pwd
import json
import os
import pandas as pd
import re

def save_to_file(data, filename):
    with open(filename, 'w') as file:
        file.write(data)

def load_from_file(filename):
    with open(filename, 'r') as file:
        data = file.read()
    return data


def extract_table(file_content, start_marker, end_marker):
    start = file_content.find(start_marker)
    end = file_content.find(end_marker, start)
    table_text = file_content[start:end].strip()
    return table_text

def clean_ansi_escape(text):
    # Regular expression to match all ANSI escape sequences
    ansi_escape = re.compile(r'''
        \x1b  # ESC
        \[    # [
        [0-?]*  # 0 or more characters in the range 0-?
        [ -/]*  # 0 or more characters in the range -/ 
        [@-~]  # a character in the range @-~
    ''', re.VERBOSE)
    return ansi_escape.sub('', text)


def parse_table(table_text):
    rows = table_text.split('\n')[2:]  # Skip the header lines
    data = []
    for row in rows:
        if row.startswith('|'):
            columns = row.split('|')[1:-1]  # Ignore the first and last empty strings
            data.append([clean_ansi_escape(col.strip()) for col in columns])
    return data


fp = "test_parse4.txt"



if os.path.exists(fp):
    print("loading file")
    x = load_from_file(fp)
else:
    s = SavioClient()
    x = s.execute_command("sq")
    print("saving file")
    save_to_file(x, fp)

dfs = []

table_text = extract_table(x, '', 'Recent jobs')
table_data = parse_table(table_text)
df = pd.DataFrame(table_data[1:], columns=table_data[0])
dfs.append(df)

table_text = extract_table(x, 'Recent jobs', '\n\n')
table_data = parse_table(table_text)
df = pd.DataFrame(table_data[1:], columns=table_data[0])
dfs.append(df)

df = pd.concat(dfs)
print(df) 
