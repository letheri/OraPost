import datetime
import json
import logging
import os

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

from libs.postgres import Postgres
from libs.oracle import Oracle


def date():
    return str(datetime.datetime.today()).split('.')[0].replace(' ', '__').replace(':', '-')


doesLogFolderExist = os.path.exists('log')
if not doesLogFolderExist:
    os.makedirs('log')
# noinspection PyArgumentList
logging.basicConfig(filename=fr'log\{date()}.log', filemode='x', format='%(name)s - %(levelname)s - %(message)s',
                    level=20, encoding="utf-8")

logging.info(f'Aktarım aracı başlatılıyor..\nTarih: ' + date())


def data_import(settings, table_parameters):
    # Veri indirmek için pandas
    # Veritabanı aktarımı için sqlalchemy
    tp = table_parameters

    # Veri okuma
    oradb = settings['ora']
    ora = Oracle(user=oradb['username'], password=oradb['password'], TNS=oradb['TNS'])
    df = ora.dataframe(f"{tp['source']['schema']}.{tp['source']['tablename']}")
    df.columns = df.columns.str.strip().str.lower()

    # Aktarım
    pgdb = settings['pg']
    engine = create_engine(f'postgresql://{pgdb["user"]}:{pgdb["pass"]}@{pgdb["ip"]}:{pgdb["port"]}/{pgdb["db"]}')
    df.to_sql(tp['target']["tablename"], engine, if_exists="replace")
    logging.info(f"{tp['target']['tablename']} tabloya {df.shape[0]} adet veri aktarıldı.")


def set_data(settings):
    for i in settings['sql']:
        try:
            pg = Postgres(**settings['pg'])
            sql = open(i['path'], 'r').read()
            pg.execMultiline(sql)
        except FileNotFoundError:
            logging.error(
                'SQL dosyasını kontrol edin. Dosya adı "afterInsert.sql" olmalıdır '
                've uygulama dosyası ile aynı konumda olmalıdır.')
            raise
        except Exception as ex:
            logging.error(f'Hata:', exc_info=True)
            raise
        else:
            logging.info('Aktarım sonrası SQL başarılı şekilde çalıştı.')
            logging.info('Tamamlandı.. Bitiş: ' + date())


try:
    config = json.loads(open("config.json", "r", encoding="utf-8").read())
    for table in config['settings']['tables']:
        data_import(config['settings'], table)
except FileNotFoundError:
    logging.error(
        'Config dosyasını kontrol edin. '
        'Dosya adı "config.json" olmalıdır ve uygulama dosyası ile aynı konumda olmalıdır.')
    raise
except json.decoder.JSONDecodeError:
    logging.error('JSON dosyası hatalıdır. Valid bir json olduğunu doğrulayınız.')
    raise
except KeyError as ex:
    logging.error('Config dosyasında anahtar bulunamadı. Anahtar: ', ex)
    raise
except sqlalchemy.exc.OperationalError:
    logging.error('Veritabanı bilgilerinde hata bulunmaktadır. Config dosyasını kontrol edin.')
    raise
except Exception as ex:
    logging.error(f'Hata: {ex}')
    raise
else:
    logging.info('Veri aktarımı başarılı..')
    set_data(config['settings'])