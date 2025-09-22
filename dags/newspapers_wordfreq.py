import logging
import os
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import task
from sklearn.datasets import fetch_20newsgroups
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
import json
import re
from pathlib import Path

N_SHARDS = 4
DATA_DIR = Path("/tmp/data")
OUTPUT_DIR = Path("/opt/airflow/")

@task(task_id="prepare_dirs")
def prepare_dirs():
    logging.info(f"Creating {DATA_DIR} and {OUTPUT_DIR}")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

@task(task_id="fetch_newgroups")
def fetch_newgroups():
    logging.info("Downloading dataset")
    documents = fetch_20newsgroups(
        subset="all",
        remove=("headers", "footers", "quotes")
    ).data
    logging.info("Cleaning dataset")
    def clean_text(text):
        text = text.lower()
        text = re.sub(r'\n', ' ', text)
        text = re.sub(r'[\'",:().?\\/!%>\[\]$f_\#\+]+|(\d+)|(-{2,}|\*|\{|\}|\||\<|\>|\^|(={2,}|\+{2,}))', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        tokens = [tok for tok in text.split() if tok not in ENGLISH_STOP_WORDS]
        return " ".join(tokens)
    
    with open(DATA_DIR / "cleaned.txt", "w") as f:
        for doc in documents:
            f.write(clean_text(doc))
            f.write("\n")

@task(task_id="split_into_shards")
def split_into_shards(n : int):
    logging.info("Reading dataset")
    with open(DATA_DIR / "cleaned.txt") as f:
        lines: list[str] = f.readlines()
        nb_lines: int = len(lines)
    
    chunk_size: float = nb_lines / n
    
    for i in range(n):
        logging.info(f"Splitting dataset {i + 1}/{n}")
        start: int = int(round(i * chunk_size))
        end: int = int(round((i + 1) * chunk_size))
        chunk: list[str] = lines[start:end]
        logging.info(f"Chunk {i}: {end-start} lines")
        with open(DATA_DIR / f"splitted{i}.txt", "w") as f:
            f.write("".join(chunk))

@task
def count_words(i: int):
    logging.info("Counting words")
    words: dict[str, int] = {}
    with open(DATA_DIR / f"splitted{i}.txt") as f:
        for word in re.split(r"\s", f.read()):
            words[word] = words.get(word, 0) + 1
    
    logging.info("Saving results")
    with open(DATA_DIR / f"result{i}.json", "w") as f:
        json.dump(words, f)

@task(task_id="aggregate_counts")
def aggregate_counts():
    logging.info("Aggregating results")
    result: dict[str, int] = {}
    for i in range(N_SHARDS):
        with open(DATA_DIR / f"result{i}.json") as f:
            for word, count in json.load(f).items():
                result[word] = result.get(word, 0) + count
    
    logging.info("Saving results")
    with open(OUTPUT_DIR / "output.tsv", "w") as f:
        f.write("word\tcount\n")
        for word, count in sorted(result.items(), key=lambda kv: kv[1], reverse=True):
            f.write(f"{word}\t{count}\n")


with DAG(dag_id="newspaper-wordfreq-task", schedule_interval=None) as dag:
    prepare_dirs_task = prepare_dirs()
    fetch_newgroups_task = fetch_newgroups()
    split_into_shards_task = split_into_shards(N_SHARDS)
    aggregate_counts_task = aggregate_counts()

    prepare_dirs_task >> fetch_newgroups_task >> split_into_shards_task

    with TaskGroup(group_id="count_shards") as count_shards:
        for i in range(N_SHARDS):
            count_task = count_words.override(task_id=f"count_words_shard_{i:02d}")(i)
            split_into_shards_task >> count_task >> aggregate_counts_task
