import pandas as pd

class SECTransformer(object):

    @classmethod
    def transform(self,year,quarter,file_prefix):
        path = f"./sec/{year}q{quarter}/{file_prefix}.txt"
        try:
            data = pd.read_csv(path,engine="c",sep="\t",error_bad_lines=False,low_memory=False)
        except:
            data = pd.read_csv(path,engine="c",sep="\t",error_bad_lines=False,encoding="ISO-8859-1",low_memory=False)
        data["year"] = year
        data["quarter"] = quarter
        return data