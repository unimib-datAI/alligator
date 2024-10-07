import argparse
import os
import re

import pandas as pd
import requests
import tqdm

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get the results of the annotations from Alligator")
    parser.add_argument(
        "--endpoint",
        type=str,
        help="Endpoint to get the results from",
        default="http://localhost:5043",
    )
    # parser.add_argument("--dataset_name", type=str, default="biodiv-cikm-2nd-turl-scratch")
    # parser.add_argument("--dataset_name", type=str, default="htr2-rn-from-scratch-turl-120k")
    parser.add_argument("--dataset_name", type=str, default="gh-end-to-end-nil")
    parser.add_argument(
        "--gt_path",
        type=str,
        help="Path to the ground truth",
        default="/home/gatoraid/alligator/datasets/gh/gt/cea_gt.csv",  # "/home/gatoraid/alligator/datasets/biodiv/gt/cea_gt.csv",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        help="Path to save the results",
        default="results.csv",
    )
    parser.add_argument(
        "--invert_row_col",
        action="store_true",
        help="Invert the row and column in the GT",
    )
    args = parser.parse_args()
    args.invert_row_col = False
    args.gt_path = os.path.expanduser(args.gt_path)
    args.output_path = os.path.expanduser(args.output_path)
    gt = pd.read_csv(args.gt_path, header=None)
    gt.columns = ["table_name", "row", "col", "qid"]
    if args.invert_row_col:
        gt.columns = ["table_name", "col", "row", "qid"]
    tables_names = gt["table_name"].unique().tolist()
    url_regex = re.compile(r"http(s)?\:\/\/www\.wikidata\.org\/(wiki|entity)\/")
    gt["qid"] = gt["qid"].map(lambda x: url_regex.sub("", x))
    gt_mapping = {
        f"{row.table_name}-{row.row}-{row.col}": {"target": row.qid}
        for row in gt.itertuples()
        if row.qid.startswith("Q")
    }
    gt_mapping_nil = {
        f"{row.table_name}-{row.row}-{row.col}": {"target": row.qid} for row in gt.itertuples() if row.qid == "NIL"
    }
    tp = 0
    all_gt = len(gt) - len(gt_mapping_nil)
    all_predicted = 0
    current_table = None
    current_table_name = None
    alligator_annotations = None
    for table_name in tqdm.tqdm(tables_names):
        if table_name != current_table_name:
            current_table_name = table_name
            response = requests.get(
                "http://localhost:5042/dataset/{}/table/{}?token=alligator_demo_2023".format(
                    args.dataset_name, current_table_name
                ),
                headers={
                    "accept": "application/json",
                    "Content-Type": "application/json",
                },
            )
            if response:
                alligator_annotations = response.json()
                alligator_annotations = alligator_annotations["semanticAnnotations"]["cea"]
            else:
                alligator_annotations = []
            for annotation in alligator_annotations:
                key = "{}-{}-{}".format(current_table_name, annotation["idRow"], annotation["idColumn"])
                # if key in gt_mapping_nil:
                #     continue
                if key not in gt_mapping:
                    continue
                predicted_qid = ""
                if len(annotation["entity"]) > 0:
                    all_predicted += 1
                    predicted_qid = annotation["entity"][0]["id"]
                if predicted_qid != "" and predicted_qid in gt_mapping[key]["target"]:
                    tp += 1
    precision = tp / all_predicted
    recall = tp / all_gt
    f1 = 2 * (precision * recall) / (precision + recall)
    print(all_gt, all_predicted)
    print("Precision: {:.4f}".format(precision))
    print("Recall: {:.4f}".format(recall))
    print("F1: {:.4f}".format(f1))
