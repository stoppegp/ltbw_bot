from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Text, String, Date, Boolean
from sqlalchemy.ext.declarative import declarative_base
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

Base = declarative_base()
class Dokument(Base):
    __tablename__ = 'dokumente'

    id = Column(String, primary_key=True)
    drucksache = Column(String)
    titel = Column(String)
    art = Column(String)
    urheber = Column(String)
    url = Column(String)
    datum = Column(Date)
    text = Column(Text)
    dl = Column(Boolean, default=False)

class Config(Base):
    __tablename__ = 'config'

    key = Column(String, primary_key=True)
    value = Column(String)

def get_entries(limit=30, offset=0):
    entries = {}

    url = "http://www.landtag-bw.de/cms/render/live/de/sites/LTBW/home/dokumente/drucksachen/contentBoxes/drucksachen.xhr?limit=" + str(limit) + "&initiativeType=&offset=" + str(
        offset)
    content = requests.get(url)

    rawentries = content.text.split("<hr")

    for rawentry in rawentries:
        try:
            bsentry = BeautifulSoup(rawentry, 'html.parser')
            subs = bsentry.find_all("li")
            drucksache = subs[0].text
            datestring = subs[1].text.replace('Datum:', '').strip()
            datum = datetime.strptime(datestring, "%d.%m.%Y")
            art = subs[2].text.replace('Art:', '').strip()
            urheber = subs[3].text.replace('Urheber:', '').strip()
            url = bsentry.a.attrs['href']
            titel = bsentry.a.text
            key = str(drucksache) + "/" + datum.strftime("%Y/%m/%d")

            entries[key] = {'drucksache': drucksache, 'art': art, 'urheber': urheber, 'url': url, 'titel': titel, 'datum': datum}
        except:
            pass
    return entries


def ltgetter(filename, start_date):
    engine = create_engine('sqlite:///' + filename)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    if (session.query(Config).filter_by(key='ldate').count() == 0):
        ldateo = Config(key='ldate', value=start_date.strftime("%Y-%m-%d"))
        session.add(ldateo)
        session.commit()

    ldateo = session.query(Config).filter_by(key='ldate').first()
    startdate = datetime.strptime(ldateo.value, "%Y-%m-%d")

    entries = {}
    offset = 0
    ldate = datetime.now()
    c = 0
    while ldate > startdate:
        print("Run " + str(c))
        entries = {**entries, **get_entries(30,offset)}
        ldate = min([x['datum'] for x in entries.values()])
        print(ldate)
        if (c > 30):
            break
        else:
            c += 1
            offset += 30

    ldateo.value = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")

    for entrykey, entry in entries.items():
        if session.query(Dokument).filter_by(id=entrykey).count() == 0:
            dokument = Dokument(id = entrykey, drucksache = entry['drucksache'], titel = entry['titel'], art = entry['art'], urheber = entry['urheber'], url = entry['url'], datum = entry['datum'])
            session.add(dokument)
    session.commit()


if __name__ == '__main__':
    ltgetter('test.db', datetime(2020,7,1))

