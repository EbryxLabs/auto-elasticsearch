from parser import Parser


class OracleParser(Parser):
    def __init__(self, rds_instances):
        super().__init__(rds_instances, ['oracle'])