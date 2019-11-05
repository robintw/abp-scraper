import pandas as pd
from sqlalchemy import create_engine


def get_ships_alongside():
    url = r'http://www.southamptonvts.co.uk/BackgroundSite/Ajax/LoadXmlFileWithTransform?xmlFilePath=D:\ftp\southampton\Ships_Along_Side\sotberthed.xml&xslFilePath=D:\wwwroot\CMS_Southampton\content\files\assets\sotberthed.xsl&w=28'
    df = pd.read_html(url, header=0)[0]

    df.columns = df.columns.map(lambda x: x.replace(' ', '_'))

    df['Actual_Arrival_Time'] = pd.to_datetime(df['Actual_Arrival_Time'])

    return df

def get_ships_sailed():
    url = r'http://southamptonvts.co.uk/BackgroundSite/Ajax/LoadXmlFileWithTransform?xmlFilePath=D:\ftp\southampton\Vessels_Sailed\sotsailed.xml&xslFilePath=D:\wwwroot\CMS_Southampton\content\files\assets\sotsailed.xsl&w=8'
    df = pd.read_html(url, header=0)[0]

    df.columns = df.columns.map(lambda x: x.replace(' ', '_'))

    df['Actual_Time_Of_Departure'] = pd.to_datetime(df['Actual_Time_Of_Departure'])

    return df

def get_ships_underway():
    url = r'http://southamptonvts.co.uk/BackgroundSite/Ajax/LoadXmlFileWithTransform?xmlFilePath=D:\ftp\southampton\Ships_On_Passage\sotonpassage.xml&xslFilePath=D:\wwwroot\CMS_Southampton\content\files\assets\sotonpassage.xsl&w=31'
    df = pd.read_html(url, header=0)[0]

    df.columns = df.columns.map(lambda x: x.replace(' ', '_'))

    df['On_Passage_Time'] = pd.to_datetime(df['On_Passage_Time'])

    return df

def get_cruise_ship_schedule():
    url = r'http://southamptonvts.co.uk/BackgroundSite/Ajax/LoadXmlFileWithTransform?xmlFilePath=D:\ftp\southampton\Cruise_Ships\DandGMediaCruiseShips.xml&xslFilePath=D:\wwwroot\CMS_Southampton\content\files\assets\sotcruiseship.xsl&w=51'
    df = pd.read_html(url, header=0)[0]

    df.columns = df.columns.map(lambda x: x.replace(' ', '_'))

    df['Departure_Time'] = pd.to_datetime(df['Departure_Time'])

    return df


eng = create_engine('mysql://ships:howmuchareshipsaffectingpollution@127.0.0.1:3306/ships')

df = get_ships_alongside()
df['timestamp'] = pd.Timestamp.now()
df.to_sql('ships_alongside_raw', eng, if_exists='append', index=False)

df = get_ships_sailed()
df['timestamp'] = pd.Timestamp.now()
df.to_sql('ships_sailed_raw', eng, if_exists='append', index=False)

df = get_ships_underway()
df['timestamp'] = pd.Timestamp.now()
df.to_sql('ships_underway_raw', eng, if_exists='append', index=False)

df = get_cruise_ship_schedule()
df['timestamp'] = pd.Timestamp.now()
df.to_sql('cruise_ship_schedule_raw', eng, if_exists='append', index=False)


