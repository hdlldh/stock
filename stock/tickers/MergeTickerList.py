import pandas as pd

ref_files = ['DowComposite_20190614.csv',
             'Nasdaq100_Top_20190614.csv',
             'Nasdaq_20190614.csv',
             'Russell1000_20190614.csv',
             'SP100_Top_20190614.csv',
             'SP400_Mid_20190614.csv',
             'SP500_Large_20190614.csv',
             'SP600_Small_20190614.csv',
             'WatchList.csv']

output = []
for file_ in ref_files:
    df = pd.read_csv(file_,usecols=['Symbol','Name'])
    if 'WatchList' not in file_:
        name = '_'.join(file_.split('_')[:-1])
    else:
        name = 'WatchList'
    df['Type'] = name
    df['Value'] = 1
    print(df.head())
    output.append(df)

output = pd.concat(output).set_index(['Symbol','Name','Type'])['Value'].unstack('Type')
output.fillna(0,inplace = True)
print(output.head())
output.to_csv('SymbolList.csv')
