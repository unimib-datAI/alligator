import argparse
import os

import pandas as pd
import requests

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get the results of the annotations from Alligator"
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        help="Endpoint to get the results from",
        default="http://localhost:5042",
    )
    parser.add_argument("--dataset_name", type=str,
                        default="htr1-2022-cikm-test")
    parser.add_argument(
        "--gt_path",
        type=str,
        help="Path to the ground truth",
        default="/home/gatoraid/alligator/datasets/hardtabler1/gt/cea_gt.csv",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        help="Path to save the results",
        default="results.csv",
    )
    args = parser.parse_args()
    args.gt_path = os.path.expanduser(args.gt_path)
    args.output_path = os.path.expanduser(args.output_path)
    gt = pd.read_csv(args.gt_path)
    gt.columns = ["table_name", "row", "col", "qid"]
    tables_names = gt["table_name"].unique().tolist()
    gt["qid"] = gt["qid"].map(
        lambda x: x.replace("http://www.wikidata.org/entity/", "")
    )
    gt_mapping = {
        f"{row.table_name}-{row.row}-{row.col}": {"target": row.qid}
        for row in gt.itertuples()
    }
    tp = 0
    all_gt = len(gt)
    all_predicted = 0
    current_table = None
    current_table_name = None
    alligator_annotations = None
    for table_name in tables_names:
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
                alligator_annotations = alligator_annotations["semanticAnnotations"][
                    "cea"
                ]
            else:
                alligator_annotations = []
            for annotation in alligator_annotations:
                key = "{}-{}-{}".format(
                    current_table_name, annotation["idRow"], annotation["idColumn"]
                )
                if key not in gt_mapping:
                    continue
                predicted_qid = ""
                if len(annotation["entity"]) > 0:
                    all_predicted += 1
                    predicted_qid = annotation["entity"][0]["id"]
                if (
                    key in gt_mapping
                    and predicted_qid != ""
                    and predicted_qid in gt_mapping[key]["target"]
                ):
                    tp += 1
    precision = tp / all_predicted
    recall = tp / all_gt
    f1 = 2 * (precision * recall) / (precision + recall)
    print(all_gt, all_predicted)
    print("Precision: {:.4f}".format(precision))
    print("Recall: {:.4f}".format(recall))
    print("F1: {:.4f}".format(f1))
