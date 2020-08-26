from sqlalchemy import Date, Boolean
from sqlalchemy.ext.declarative import declarative_base
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
from sqlalchemy import create_engine, exists
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Text
import mattermost
import requests
from pathlib import Path
import os
import pdfplumber
import ltbw_bot_config as cfg
import hashlib

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

class MattermostMapping(Base):
    __tablename__ = 'mattermost_mapping_' + hashlib.md5((cfg.mattermost_url + cfg.mattermost_channelid).encode('utf-8')).hexdigest()

    id = Column(String, primary_key=True)
    drucksache = Column(String)
    mm_id = Column(String)
    mm_root_id = Column(String)

class DokumentText(Base):
    __tablename__ = 'dokumenttexte'

    id = Column(String, primary_key=True)
    drucksache = Column(String)
    text = Column(Text)

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


def ltgetter(engine, start_date):
    print("Looking for new documents...")

    # start database session
    Session = sessionmaker(bind=engine)
    session = Session()

    # check id db entry for last execution day exists, otherways create
    if (session.query(Config).filter_by(key='ldate').count() == 0):
        ldateo = Config(key='ldate', value=start_date.strftime("%Y-%m-%d"))
        session.add(ldateo)
        session.commit()

    ldateo = session.query(Config).filter_by(key='ldate').first()
    startdate = datetime.strptime(ldateo.value, "%Y-%m-%d")

    print("Look for entries since " + startdate.strftime("%Y-%m-%d"))

    entries = {}
    offset = 0
    ldate = datetime.now()
    c = 0
    while ldate > startdate:
        print("Run " + str(c+1))
        entries = {**entries, **get_entries(30,offset)}
        ldate = min([x['datum'] for x in entries.values()])
        print("Oldest entry is from " + ldate.strftime("%Y-%m-%d"))
        if (c > 30):
            print("WARNING: Maximum of 30 runs exceeded! Stopping.")
            break
        else:
            c += 1
            offset += 30

    ldateo.value = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")

    print(str(len(entries)) + " Documents found.")

    c = 0
    for entrykey, entry in entries.items():
        if session.query(Dokument).filter_by(id=entrykey).count() == 0:
            dokument = Dokument(id = entrykey, drucksache = entry['drucksache'], titel = entry['titel'], art = entry['art'], urheber = entry['urheber'], url = entry['url'], datum = entry['datum'])
            session.add(dokument)
            c += 1
        session.commit()
    print(str(c) + " new Documents added.")
    print("Looking for documents finished.")
    print("---")

def downloader(engine, start_date, folderpath):

    print("Start downloader...")

    Session = sessionmaker(bind=engine)
    session = Session()

    entries0 = session.query(Dokument)\
        .filter(~ exists().where(Dokument.id == DokumentText.id)).filter(Dokument.datum >= start_date).order_by(Dokument.datum)

    print(str(entries0.count()) + " Documents to download. Limiting to 5.")
    entries = entries0.limit(5)

    c = 0
    for entry in entries:
        c += 1
        drucksache = entry.drucksache
        id = entry.id
        print("Downloading " + str(id) + "(" + str(c) + ")...")
        try:
            url = "https://www.landtag-bw.de" + entry.url
            r = requests.get(url)
            path = folderpath + "/" + id + ".pdf"
            Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
            with open(path, 'wb') as f:
                f.write(r.content)

            print("Extracting text...")
            pdf = pdfplumber.open(path)
            text = ""
            c = 1
            for page in pdf.pages:
                text += "-- SEITE " + str(c) + " --\n\n" + page.extract_text() + "\n\n"
                c += 1
            dokumenttext = DokumentText(id=entry.id, drucksache=drucksache, text=text)
            session.add(dokumenttext)

            session.commit()
        except Exception as e:
            print("Download failed.")
            print(e)
    print("Downloading finished.")
    print("---")


def mattermost_adapter(engine, mattermost_url, mattermost_user, mattermost_password, mattermost_channelid, start_date):
    print("Mattermost Adapter started.")
    Session = sessionmaker(bind=engine)
    session = Session()

    entries0 = session.query(Dokument)\
        .filter(~ exists().where(Dokument.id == MattermostMapping.id)).filter(Dokument.datum >= start_date).order_by(Dokument.datum)

    print(str(entries0.count()) + " Documents need to be posted to Mattermost. (Limiting to 10.)")

    entries = entries0.limit(10)

    print("Connecting to Mattermost...")
    mm = mattermost.MMApi("https://" + mattermost_url + "/api")
    mm.login(mattermost_user, mattermost_password)
    c = 0
    for entry in entries:
        c += 1
        print("Posting Document " + str(entry.id) + "(" + str(c) + ")...")
        drucksache = entry.drucksache
        related = session.query(MattermostMapping).filter_by(drucksache=drucksache).first()
        if (related != None):
            print("Document is an Update")
            mm_root_id = related.mm_root_id
            pref = "Update"
        else:
            print("Document is new")
            mm_root_id = None
            pref = "Neu"

        text = pref + ": " + entry.art + ' ' + drucksache + ' von ' + entry.urheber + " (" + str(entry.datum) + ")\n"
        text += "[**" + entry.titel + "**](https://www.landtag-bw.de" + entry.url + ")"

        print("Creating post...")
        post = mm.create_post(mattermost_channelid, text, root_id = mm_root_id)
        post_id = post['id']

        if (mm_root_id == None):
            mm_root_id = post_id
        else:
            print("Getting reactions...")
            rootpost = mm.get_post(mm_root_id)
            rusers = set([])
            try:
                for reaction in rootpost['metadata']['reactions']:
                    rusers = rusers + reaction['user_id']

                mtext = ""
                for ruser in rusers:
                    username = mm.get_user(ruser)['username']
                    mtext += "@" + username + " "
                print("Post mentions")
                mm.create_post(mattermost_channelid, mtext, root_id=mm_root_id)
            except:
                print("No reactions found.")

        mmmap = MattermostMapping(id=entry.id, drucksache=drucksache, mm_id=post_id, mm_root_id=mm_root_id)
        session.add(mmmap)
        session.commit()

    print("Mattermost Update finished.")
    print("---")

if __name__ == '__main__':


    engine = create_engine('sqlite:///' + cfg.filename)
    Base.metadata.create_all(engine)

    last_execution_getter = datetime(1970,1,1,0,0,0)
    last_execution_downloader = datetime(1970, 1, 1, 0, 0, 0)
    last_execution_mattermost = datetime(1970, 1, 1, 0, 0, 0)

    errorcount_connection = 0
    try:
        while True:
            try:
                if (datetime.now() - last_execution_getter).total_seconds() > cfg.interval_getter:
                    last_execution_getter = datetime.now()
                    ltgetter(engine, cfg.startdate)
                if (datetime.now() - last_execution_downloader).total_seconds() > cfg.interval_downloader:
                    last_execution_downloader = datetime.now()
                    downloader(engine, cfg.startdate, cfg.download_path)
                if (datetime.now() - last_execution_mattermost).total_seconds() > cfg.interval_mattermost:
                    last_execution_mattermost = datetime.now()
                    mattermost_adapter(engine, cfg.mattermost_url, cfg.mattermost_user, cfg.mattermost_password, cfg.mattermost_channelid, cfg.startdate)
                errorcount_connection = 0
            except requests.exceptions.ConnectionError as e:
                print("ERROR: Could not connect to server.")
                print(e)
                errorcount_connection += 1
                if (errorcount_connection > 10):
                    raise Exception("Too many connection errors. Config error?")
            except Exception as e:
                print("ERROR: Unknown error.")
                print(e)
                raise Exception("Unknown error.")

            time.sleep(10)
    except Exception as e:
        print("FATAL ERROR: " + str(e) + " Exiting.")