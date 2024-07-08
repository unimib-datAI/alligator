from process.wrapper.Database import MongoDBWrapper


class TableAnnotations():

    def __init__(self, dataset_name: str, table_id: str):
        self.mongoDBWrapper = MongoDBWrapper()
        self.row_c = self.mongoDBWrapper.get_collection("row")
        self.candidate_scored_c = self.mongoDBWrapper.get_collection(
            "candidateScored")
        self.cea_c = self.mongoDBWrapper.get_collection("cea")
        self.cpa_c = self.mongoDBWrapper.get_collection("cpa")
        self.cta_c = self.mongoDBWrapper.get_collection("cta")
        self.dataset_c = self.mongoDBWrapper.get_collection("dataset")
        self.table_c = self.mongoDBWrapper.get_collection("table")
        self.dataset_name = dataset_name
        self.table_id = table_id

    def get_table(self, page=None):
        query = {"datasetName": self.dataset_name, "tableName": self.table_id}
        if page is not None:
            query["page"] = page
        results = self.row_c.find(query)
        out = [
            {
                "datasetName": result["datasetName"],
                "tableName": result["tableName"],
                "header": result["header"],
                "rows": result["rows"],
                "semanticAnnotations": {"cea": [], "cpa": [], "cta": []},
                "metadata": result.get("metadata", []),
                "status": result["status"]
            }
            for result in results
        ]

        if len(out) == 0:
            return {"status": "Error", "message": "Table not found"}, 404

        buffer = out[0]
        for o in out[1:]:
            buffer["rows"] += o["rows"]
        buffer["nrows"] = len(buffer["rows"])

        if len(out) > 0:
            if page is None:
                out = buffer
            else:
                out = out[0]
            doing = True
            results = self.cea_c.find(query)
            total = self.cea_c.count_documents(query)
            if total == len(out["rows"]):
                doing = False
            for result in results:
                winning_candidates = result["winningCandidates"]
                for id_col, candidates in enumerate(winning_candidates):
                    entities = []
                    for candidate in candidates[0:3]:
                        entities.append({
                            "id": candidate["id"],
                            "name": candidate["name"],
                            "type": candidate["types"],
                            "description": candidate["description"],
                            "match": candidate["match"],
                            "score": candidate.get("rho'"),
                            "features": [
                                {"id": "delta",
                                    "value": candidate.get("delta")},
                                {"id": "omega",
                                    "value": candidate.get("score")},
                                {"id": "levenshtein_distance",
                                    "value": candidate["features"].get("ed_score")},
                                {"id": "jaccard_distance", "value": candidate["features"].get(
                                    "jaccard_score")},
                                {"id": "popularity",
                                    "value": candidate["features"].get("popularity")}
                            ]
                        })
                    out["semanticAnnotations"]["cea"].append({
                        "idColumn": id_col,
                        "idRow": result["row"],
                        "entity": entities
                    })
            out["status"] = "DONE" if doing is False else "DOING"
            result = self.cpa_c.find_one(query)
            if result is not None:
                winning_predicates = result["cpa"]
                for id_source_column in winning_predicates:
                    for id_target_column in winning_predicates[id_source_column]:
                        out["semanticAnnotations"]["cpa"].append({
                            "idSourceColumn": id_source_column,
                            "idTargetColumn": id_target_column,
                            "predicate": winning_predicates[id_source_column][id_target_column]
                        })

            result = self.cta_c.find_one(query)
            if result is not None:
                winning_types = result["cta"]
                for id_col in winning_types:
                    out["semanticAnnotations"]["cta"].append({
                        "idColumn": int(id_col),
                        "types": [winning_types[id_col]]
                    })
        return out
