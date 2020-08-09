from sqlalchemy import create_engine, exists
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String
from ltgetter import Base, Dokument
import hashlib
import mattermost
from datetime import datetime
from ltbw_bot_config import *


class MattermostMapping(Base):
    __tablename__ = 'mattermost_mapping_' + hashlib.md5((mattermost_url+mattermost_channelid).encode('utf-8')).hexdigest()

    id = Column(String, primary_key=True)
    drucksache = Column(String)
    mm_id = Column(String)
    mm_root_id = Column(String)



def mattermost_adapter(filename, start_date):
    engine = create_engine('sqlite:///' + filename)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    entries = session.query(Dokument)\
        .filter(~ exists().where(Dokument.id == MattermostMapping.id)).filter(Dokument.datum >= start_date).order_by(Dokument.datum)

    session.commit()
    mm = mattermost.MMApi("https://" + mattermost_url + "/api")
    mm.login(mattermost_user, mattermost_password)

    for entry in entries:
        drucksache = entry.drucksache
        related = session.query(MattermostMapping).filter_by(drucksache=drucksache).first()
        if (related != None):
            mm_root_id = related.mm_root_id
            pref = "Update"
        else:
            mm_root_id = None
            pref = "Neu"

        text = pref + ": " + entry.art + ' ' + drucksache + ' von ' + entry.urheber + " (" + str(entry.datum) + ")\n"
        text += "[**" + entry.titel + "**](https://www.landtag-bw.de" + entry.url + ")"

        post = mm.create_post(mattermost_channelid, text, root_id = mm_root_id)
        post_id = post['id']

        if (mm_root_id == None):
            mm_root_id = post_id
        else:
            rootpost = mm.get_post(mm_root_id)
            rusers = set([])
            for reaction in rootpost['metadata']['reactions']:
                rusers = rusers + reaction['user_id']

            mtext = ""
            for ruser in rusers:
                username = mm.get_user(ruser)['username']
                mtext += "@" + username + " "
            mm.create_post(mattermost_channelid, mtext, root_id=mm_root_id)

        mmmap = MattermostMapping(id=entry.id, drucksache=drucksache, mm_id=post_id, mm_root_id=mm_root_id)
        session.add(mmmap)


    session.commit()

if __name__ == '__main__':
    mattermost_adapter('test.db', datetime(2020,7,1))

