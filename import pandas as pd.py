import pandas as pd
file = "text.xls"
a = {"123":[2],"321":[4]}
with pd.ExcelWriter(path=file, mode="w") as writer:
    pd.DataFrame(a).to_excel(writer)