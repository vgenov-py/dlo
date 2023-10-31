import sqlite3
from uuid import uuid5
from functools import reduce
import re
from os import system

class DLO:
    def __init__(self, db_file:str, dataset:str,  args:dict=None):
        self.db_file = db_file
        self.dataset = dataset
        self.args = args

    @property
    def con(self):
        return sqlite3.connect(self.db_file)
    
    @property
    def cur(self):
        return self.con.cursor()
    
    @property
    def available_fields(self) -> list:
        return [row[1] for row in self.cur.execute(f"pragma table_info({self.dataset});")]
    
    @property
    def datasets(self):
        return list(map(lambda name:name[0],filter(lambda name: len(name[0]) == 3, self.cur.execute("select name from sqlite_master where type='table';"))))
    
    @property
    def purgue(self) -> dict:
        result = {}
        error = {"success": False}
        args = self.args.copy()
        fields = args.pop("fields", None)
        limit = args.pop("limit", "1000")
        group_by = args.pop("group_by", None)
        order_by = args.pop("order_by", None)
        filters = args.items()

        if limit:
            try:
                int(limit)
            except ValueError:
                error["message"] = f"Limit shoulb be an integer"
                return error
        result["limit"] = limit

        if group_by:
            if group_by not in self.available_fields:
                error["message"] = "Can't group by no existing field"
                return error
            result["group_by"] = group_by

        if order_by:
            order_key = order_by.split(",")[0]
            if order_key not in self.available_fields:
                result["message"] = f"Can't order by {order_key}"
                return error
            try:
                order_direction:str = order_by.split(",")[1].strip()
                if order_direction not in ("asc", "desc"):
                    error["message"] = f"Ascending order asc, descending desc"
                    return error
            except Exception:
                pass
        result["order_by"] = order_by

        for field in fields.split(",") if fields else ():
            fields = fields.replace(field, field.strip())
            field:str = field.strip()
            if field not in self.available_fields:
                error["message"] = f"Field not available"
                return error        
       
        not_available_filter = next(filter(lambda kv: kv[0] not in self.available_fields, filters), None)
        if not_available_filter:
            error["message"] = f"Filter not available in available fields"
            return error
        result["filters"] = dict(filters)
        
        if fields:        
            result["fields"] = fields
        else:
            result["fields"] = reduce(lambda x,y:f"{x},{y}", self.available_fields)

        result["success"] = True
        return result
    
    def where(self, args: dict, dataset:str = None) -> str:
        '''In args the filters should be received'''
        args = dict(args)
        dataset = dataset if dataset else self.dataset
        if not args:
            return ""
        result = f"WHERE "
        and_or = " AND "
        for k,value in args.items():
            v = value.strip()
            v = v.replace("|","")
            v = v.replace("!","")
            if v.find("null") >= 0:
                v_where = f'''{dataset}.{k} IS NULL{and_or}'''
                if v.find("!") >= 0:
                    v_where = v_where.replace("IS NULL", "IS NOT NULL")
            elif v.find("<") >= 0:
                v = v.replace("<", "")
                v_where = f'''{k} >= '{v}' {and_or}'''
            elif v.find(">") >= 0:
                v = v.replace(">", "")
                v_where = f'''{k} <= '{v}' {and_or}'''
            elif re.search("\d{4}\-\d{4}", v):
                s, e = v.split("-")
                v_where = f'''{k} BETWEEN '{s}' AND '{e}' {and_or}'''
            else:
                v_where = f'''{dataset}.{k} MATCH '{v}*'{and_or}'''


            result += v_where
        return result[:-5]
        
    def query(self, count=False, limit=True) -> str:
        fields = self.purgue.get("fields")
        order_by = self.purgue.get("order_by")
        group_by = self.purgue.get("group_by")
        filters = self.purgue.get("filters")

        if count:
            query = f"SELECT count(*) FROM {self.dataset} "
        else:
            if fields:
                query = f"SELECT {fields} FROM {self.dataset} "
            else:
                query = f"SELECT * FROM {self.dataset} "

        query += self.where(filters.items())    
        if order_by:
            if order_by.find(",") >= 0:
                order_by = order_by.replace(",", " ")
            query += f" ORDER BY {order_by} "
        if group_by:
            query += f"GROUP BY {group_by}"
        if limit:
            return query + " LIMIT 1000;"
        return query
    
    def create(self, values) -> bool:
        query = f"INSERT INTO {self.dataset} VALUES ({('?, '*len(self.available_fields))[:-2]})"
        try:
            self.cur.execute(query, [uuid5().hex] + values + ["0002"])
            self.con.commit()
            return True
        except Exception as e:
            return False
    
    def json(self) -> dict:
        res_json = self.purgue
        if not res_json["success"]:
            return {"success":False,"message":res_json["message"]}
        
        query = self.query()   
                    
        try:
            res = self.cur.execute(query)

        except sqlite3.OperationalError as e:
            res = {}
            res["success"] = False
            res["message"] = "SQLite3 Operational Error"
            res["error"] = f"{e}"
            if res["error"] == 'fts5: syntax error near ""':
                res["message"] += ": La búsqueda no ha entregado resultados en el dataset 1"
            return res
        
        result = {}
        result["success"] = True
        fields = self.purgue.get("fields").split(",")
        result["data"] = map(lambda row:dict(zip(fields, row)),res)
        result["fields"] = fields
        return result
    
    def export_csv(self, file_destination:str=None) -> str:
        from secrets import token_hex
        file_destination = file_destination if file_destination else f"{self.dataset}_{token_hex}.csv"
        query = f'sqlite3 {self.db_file} -header -csv -separator ";" " {self.query(limit=False)} " > {file_destination}'
        system(query)
        return file_destination
    
    def export_json(self, file_destination:str=None) -> str:
        from secrets import token_hex
        file_destination = file_destination if file_destination else f"{self.dataset}_{token_hex}.csv"
        query = f'''sqlite3 {self.db_file} -json " {self.query(count=False, limit=False)} " > {file_destination}'''
        system(query)
        return file_destination
    
if __name__ == "__main__": 
    import unittest

    class test_QLO(unittest.TestCase):

        def setUp(self) -> None:
            self.n_0 =DLO("src/test.db", "dogs", {"id":"0001"})

        def test_available_fields(self):
            self.assertEqual(self.n_0.available_fields, ["id", "name", "age"])
        
        def test_where(self):
            self.assertEqual(self.n_0.where({"id": "0001"}), "WHERE dogs.id MATCH '0001*'")
        
        def test_query(self):
            self.assertEqual(self.n_0.query(), "SELECT id,name,age FROM dogs WHERE dogs.id MATCH '0001*' LIMIT 1000;")

        def test_export_csv(self):
            self.assertEqual(self.n_0.export_csv("test.csv"), "test.csv")

        def test_export_csv(self):
            self.assertEqual(self.n_0.export_json("test.json"), "test.json")
        

    unittest.main()