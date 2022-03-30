import csv
import random

import pandas


def swap_columns(df):
    return df[['ind', 'name', 'age', 'position']]


async def async_main_task(df: pandas.DataFrame):
    df = df.replace("Артур", "Король Артур").replace('Гриша', "Григорий Лепс")
    return swap_columns(df)


def main_task(df: pandas.DataFrame):
    df = df.replace("Артур", "Король Артур").replace('Гриша', "Григорий Лепс")
    return swap_columns(df)


names = ['Гриша', 'Артур']


def generate_csv_file(rows=10000):
    with open("benchmark.csv", 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['ind', 'age', 'name', 'position'])
        for i in range(rows):
            writer.writerow([
                str(i), str(random.randint(16, 48)), random.choice(names), 'position'
            ])


def split_df():
    df = pandas.read_csv("benchmark.csv")
    rows = []
    left = 0
    right = 200
    for _ in range(5):
        rows.append(
            df.loc[left:right, :]
        )
        left += 200
        right += 200
    return rows
