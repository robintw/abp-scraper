import pandas as pd
from sqlalchemy import create_engine


def get_ships_alongside():
    url = r'http://www.southamptonvts.co.uk/BackgroundSite/Ajax/LoadXmlFileWithTransform?xmlFilePath=D:\ftp\southampton\Ships_Along_Side\sotberthed.xml&xslFilePath=D:\wwwroot\CMS_Southampton\content\files\assets\sotberthed.xsl&w=28'
    df = pd.read_html(url, header=0)[0]

    df.columns = df.columns.map(lambda x: x.replace(' ', '_'))

    df['Actual_Arrival_Time'] = pd.to_datetime(df['Actual_Arrival_Time'])

    return df


eng = create_engine('mysql://ships:howmuchareshipsaffectingpollution@127.0.0.1:3306/ships')

df = get_ships_alongside()

df['timestamp'] = pd.Timestamp.now()

df.to_sql('ships_alongside_raw', eng, if_exists='append', index=False)
