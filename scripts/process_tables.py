import json
import os
import re

import pandas as pd
import requests
from tqdm import tqdm


def clean_str(value):
    value = str(value)
    stop_charaters = ["_"]
    for char in stop_charaters:
        value = value.replace(char, " ")
    value = " ".join(value.split()).lower()
    return value


def cea_process(
    cea_target_path,
    separator=",",
    invert_rows_cols: bool = False,
    drop_nil: bool = False,
):
    url_regex = re.compile(r"http(s)?\:\/\/www\.wikidata\.org\/(wiki|entity)\/")
    cea_gt = pd.read_csv(cea_target_path, sep=separator, header=None)
    if drop_nil:
        cea_gt = cea_gt[cea_gt[3] != "NIL"]
        cea_gt = cea_gt[cea_gt[3] != ""]
    cea_gt.iloc[:, -1] = cea_gt.iloc[:, -1].apply(lambda x: url_regex.sub("", x))
    cea_gt[1] = cea_gt[1].astype(int)
    cea_gt[2] = cea_gt[2].astype(int)
    NE_cols = (
        set()
    )  # to store couple of (id_table, id_col) for tracing NE cols to be annotated!
    cell_to_entity = {}
    if invert_rows_cols:
        cea_gt_np = cea_gt.to_numpy()
        cea_gt_np[:, [1, 2]] = cea_gt_np[:, [2, 1]]
        cea_gt = pd.DataFrame(cea_gt_np, columns=cea_gt.columns)
    for _, row in tqdm(cea_gt.iterrows(), total=len(cea_gt)):
        id_table, id_row, id_col, entity = row
        cell_to_entity[f"{id_table} {id_row} {id_col}"] = entity
        NE_cols.add(f"{id_table} {id_col}")
    minimum_row_is_zero = False
    if cea_gt.iloc[:, 1].min() == 0:
        minimum_row_is_zero = True

    return cell_to_entity, NE_cols, minimum_row_is_zero


def extract_qids_from_urls(url_list):
    qid_list = []
    for url in url_list:
        # Regular expression to match QIDs in the URL
        qid_match = re.search(r"Q\d+", url)
        if qid_match:
            qid_list.append(qid_match.group())
    return qid_list


def generate_api_format(
    id_dataset,
    tables_path,
    cell_to_entity,
    NE_cols,
    minimum_row_is_zero,
    separator=";",
    include_ids=False,
    header="infer",
    columns_to_exclude=[],
):
    buffer = []
    tables = os.listdir(tables_path)
    for table in tqdm(tables):
        name = os.path.splitext(table)[0]
        if table.startswith("."):
            continue
        try:
            df = pd.read_csv(
                f"{tables_path}/{table}", sep=separator, header=header
            )  # Change delimiter if different
            df = df.drop(columns=columns_to_exclude)
            if header is None:
                df.columns = ["col" + str(i) for i in range(len(df.columns))]
        except pd.errors.ParserError:
            print(f"Error parsing {tables_path}/{table}")
            continue
        for column in df.columns:
            if df[column].dtype == "float64":
                # For float columns, fill with 0 or another appropriate value like df[column].mean()
                df[column] = df[column].fillna(0)
            else:
                # For object columns, fill with an empty string or another appropriate placeholder
                df[column] = df[column].fillna("")
        json_data = json.loads(df.to_json(orient="split"))
        table = {
            "datasetName": id_dataset,
            "tableName": name,
            "header": list(df.columns),
            "rows": [],
            "semanticAnnotations": {},
            "metadata": {"column": []},
            "lamapi_kwargs": {
                "kg": "wikidata",
                "limit": 50,
                "kind": "entity",
                "language": "en",
                "cache": False,
            },
        }
        rows = json_data["data"]
        id_row = 1
        if minimum_row_is_zero:
            id_row = 0
        for row in rows:
            table["rows"].append(
                {
                    "idRow": id_row,
                    "data": [str(cell) for cell in row],
                    "ids": [
                        str(cell_to_entity.get(f"{name} {id_row} {id_col}", ""))
                        for id_col, _ in enumerate(row)
                    ],
                }
                if include_ids
                else {"idRow": id_row, "data": [str(cell) for cell in row]}
            )
            id_row += 1

        for id_col, row in enumerate(rows[0]):
            key = f"{name} {id_col}"
            if key in NE_cols:
                table["metadata"]["column"].append({"idColumn": id_col, "tag": "NE"})

        buffer.append(table)
    return buffer


if __name__ == "__main__":
    datasets = {
        # Parameters:
        # gt separator: ","
        # tables separator: ";"
        # invert_rows_cols: False
        # include_ids: True
        # header: "infer"
        # "turl-2k-rn-from-scratch-turl-120k-no-correct-qids": {
        #     "tables": "/home/gatoraid/alligator/datasets/turl-2k/tables",
        #     "cea": "/home/gatoraid/alligator/datasets/turl-2k/gt/cea.csv",
        #     "cpa": "",
        #     "cta": "",
        # },
        # Parameters:
        # gt separator: ","
        # tables separator: ","
        # invert_rows_cols: False
        # include_ids: True
        # header: "infer"
        # "turl-120k-correct-qid": {
        #     "tables": "/home/gatoraid/alligator/datasets/turl-120k/tables",
        #     "cea": "/home/gatoraid/alligator/datasets/turl-120k/gt/cea.csv",
        #     "cpa": "",
        #     "cta": "",
        # },
        # Parameters:
        # gt separator: ","
        # tables separator: ","
        # invert_rows_cols: False
        # include_ids: True
        # header: "infer"
        # "htr1-baseline-no-qids": {
        #     "tables": "/home/gatoraid/alligator/datasets/hardtabler1/tables",
        #     "cea": "/home/gatoraid/alligator/datasets/hardtabler1/gt/cea_gt.csv",
        #     "cpa": "",
        #     "cta": "",
        # },
        # "htr2-rn-from-scratch-turl-120k-correct-qids": {
        #     "tables": "/home/gatoraid/alligator/datasets/hardtabler2/tables",
        #     "cea": "/home/gatoraid/alligator/datasets/hardtabler2/gt/cea_gt.csv",
        #     "cpa": "",
        #     "cta": "",
        # },
        # "2t-baseline": {
        #     "tables": "/home/gatoraid/alligator/datasets/2t/tables",
        #     "cea": "/home/gatoraid/alligator/datasets/2t/gt/cea_gt.csv",
        #     "cpa": "",
        #     "cta": "",
        # },
        # "wdt-r1-2023-baseline": {
        #     "tables": "/home/gatoraid/alligator/datasets/wikidatatables2023r1/tables",
        #     "cea": "/home/gatoraid/alligator/datasets/wikidatatables2023r1/gt/cea_gt.csv",
        #     "cpa": "",
        #     "cta": "",
        # },
        # Parameters:
        # gt separator: ","
        # tables separator: ","
        # invert_rows_cols: True
        # include_ids: True
        # header: None
        # "biodiv-rn-from-scratch-turl-120k-no-correct-qids": {
        #     "tables": "/home/gatoraid/alligator/datasets/biodiv/tables",
        #     "cea": "/home/gatoraid/alligator/datasets/biodiv/gt/cea_gt.csv",
        #     "cpa": "",
        #     "cta": "",
        # },
        # Parameters:
        # gt separator: ","
        # tables separator: ","
        # invert_rows_cols: False
        # include_ids: True
        # header: infer
        # columns_to_exclude: ["HIL_total"]
        # "sn-linker-nil": {
        #     "tables": "/home/gatoraid/alligator/datasets/sn/tables",
        #     "cea": "/home/gatoraid/alligator/datasets/sn/gt/cea_gt.csv",
        #     "cpa": "",
        #     "cta": "",
        # },
        # Parameters:
        # gt separator: ","
        # tables separator: ","
        # invert_rows_cols: False
        # include_ids: False
        # header: infer
        # columns_to_exclude: ["idd", "id"]
        "gh-linker-nil": {
            "tables": "/home/belerico/projects/alligator/datasets/gh/tables",
            "cea": "/home/belerico/projects/alligator/datasets/gh/gt/cea_gt.csv",
            "cpa": "",
            "cta": "",
        },
    }

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    params = (("token", "alligator_demo_2023"), ("kg", "wikidata"))

    for dataset in datasets:
        tables_path, cea_target_path, cpa_target_path, cta_target_path = list(
            datasets[dataset].values()
        )
        cell_to_entity, NE_cols, minimum_row_is_zero = cea_process(
            cea_target_path,
            separator=",",
            invert_rows_cols=False,
        )
        buffer = generate_api_format(
            dataset,
            tables_path,
            cell_to_entity,
            NE_cols,
            minimum_row_is_zero,
            separator=",",
            include_ids=True,
            header="infer",
            columns_to_exclude=["idd", "id"],
        )
        response = requests.post(
            "http://127.0.0.1:5042//dataset/createWithArray",
            headers=headers,
            params=params,
            json=buffer,
        )
