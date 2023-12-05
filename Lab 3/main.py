# import boto3
import os
import json
import csv
from flatten_json import flatten

def process_and_flatten_json(json_file_path):
    with open(json_file_path, 'r') as file_json:
        data = json.load(file_json)
        flattened_data = flatten(data, separator='_')
        return flattened_data

def convert_single_json_to_csv(json_file_path, csv_file_path):
    json_data_flattened = process_and_flatten_json(json_file_path)

    with open(csv_file_path, 'w', newline='') as file_csv:
        csv_writer = csv.writer(file_csv)

        # Записати заголовки (назви стовпців)
        csv_writer.writerow(json_data_flattened.keys())

        # Записати значення у відповідні стовпці
        csv_writer.writerow(json_data_flattened.values())

def convert_json_folder_to_csv(json_folder_path, csv_folder_path):
    if not os.path.exists(csv_folder_path):
        os.makedirs(csv_folder_path)

    for root, dirs, files in os.walk(json_folder_path):
        for json_file_name in files:
            if json_file_name.endswith('.json'):
                json_file_path = os.path.join(root, json_file_name)
                csv_file_path = os.path.join(csv_folder_path, json_file_name.replace('.json', '.csv'))

                convert_single_json_to_csv(json_file_path, csv_file_path)


def main_execution():
    path_json_folder = './data'
    path_csv_folder = './csv_output'

    convert_json_folder_to_csv(path_json_folder, path_csv_folder)

if __name__ == "__main__":
    main_execution()
