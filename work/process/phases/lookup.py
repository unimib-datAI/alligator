import traceback
from model.row import Row


class Lookup:
    def __init__(self, data:object, lamAPI, target, log_c, kg_ref="wikidata", limit=100):
        self._header = data.get("header", [])
        self._dataset_name = data["datasetName"]
        self._table_name = data["tableName"]
        self._types = data["types"]     
        self._lamAPI = lamAPI
        self._target = target
        self._log_c = log_c
        self._kg_ref = kg_ref
        self._limit = limit
        self._rows = []
        for row in data["rows"]:
            row = self._build_row(row["data"], row["idRow"])
            self._rows.append(row)


    def _build_row(self, cells, id_row):
        row = Row(id_row, len(cells))
        row_text = " ".join([str(cell) for cell in cells])
        for i, cell in enumerate(cells):
            if i in self._target["NE"]:
                types = self._types.get(str(i))
                candidates = self._get_candidates(cell, id_row, types)
                is_subject = i == self._target["SUBJ"]
                row.add_ne_cell(cell, row_text, candidates, i, is_subject)
            elif i in self._target["LIT"]:
                row.add_lit_cell(cell, i, self._target["LIT_DATATYPE"][str(i)])
            else:    
                row.add_notag_cell(cell)
        return row

    
    def _get_candidates(self, cell, id_row, types):
        candidates = []
        result = None
        try:
            if len(str(cell)) > 0 and str(cell).lower() != "nan":
                result = self._lamAPI.lookup(cell, ngrams=True, fuzzy=False, types=types, limit="special")
                if cell not in result:
                    raise Exception("Error from lamAPI")
            candidates = result[cell]    
        except Exception as e:
            self._log_c.insert_one({
                'datasetName': self._dataset_name,
                'tableName': self._table_name,
                'idRow': id_row,
                'cell': cell,
                'types': types,
                'error': str(e), 
                'stackTrace': traceback.format_exc(),
                'result': result
            })
            return []
            
        return candidates

    
    def get_rows(self):
        return self._rows