import sys
import time
from threading import Timer
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sqlalchemy import Column, Integer, String, DateTime, create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import requests


engine = create_engine('sqlite:///medias.db')
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()
DIR_MONITORED = './monitored' # Define qual diretório será monitorado


# ============================================================================
# Classe modelo para a tabela media
# Utilizada para importar os registros de asrun Log
# ============================================================================
class Media(Base):
    __tablename__ = 'media'
    __table_args__ = {'sqlite_autoincrement': True}

    id = Column(Integer, primary_key=True)
    start_time = Column(String(), nullable=True)
    end_time = Column(String(), nullable=True)
    title = Column(String(), nullable=True)
    duration = Column(String(), nullable=True)
    reconcile_key = Column(String(), nullable=True)
    job_id = Column(String(), nullable=True)
    status = Column(String(), nullable=True)
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)

    def duration_in_seconds(self):
        duration = self.duration[0:8]
        parse_duration = time.strptime(duration,'%H:%M:%S')

        secs = timedelta(
            hours=parse_duration.tm_hour,
            minutes=parse_duration.tm_min,
            seconds=parse_duration.tm_sec
            ).total_seconds()

        return int(secs)


# ============================================================================
# Classe modelo para a tabela file_control
# Utilizada para registrar os arquivos importados e a quantidade de linhas
# ============================================================================
class FileControl(Base):
    __tablename__ = 'file_control'
    __table_args__ = {'sqlite_autoincrement': True}

    id = Column(Integer, primary_key=True)
    filename = Column(String, nullable=True)
    total_lines = Column(Integer, nullable=True)
    created_at = Column(DateTime, nullable=True)


# ============================================================================
# Cria banco de dados, caso não exista
# ============================================================================
Base.metadata.create_all(engine)


# ============================================================================
# Classe para lidar com os eventos de sistema de arquivo
# A cada arquivo adicionado no diretório monitorado, é executado o método
# para processar o arquivo.
# ============================================================================
class Handler(FileSystemEventHandler):

    patterns = ["*.txt"]

    def process(self, event):
        if event.event_type == 'created':
            process_file(event.src_path)

    def on_created(self, event):
        self.process(event)


# ============================================================================
# Método para extrair trechos do arquivo adicionado.
# Retorna dicionário com as informações relevantes
# ============================================================================
def extract_fields(line):
    dic = {
        "start_time": line[6:28].decode('utf-8'),
        "end_time": line[29:51].decode('utf-8'),
        "title": line[106:138].decode('utf-8'),
        "duration": line[184:195].decode('utf-8'),
        "reconcile_key": line[279:344].decode('utf-8')
    }

    return dic


# ============================================================================
# Método para checar a cada 10 segundos se o job foi concluído.
# Caso o job tenha finalizado, grava no registro correspondente o status atual
# ============================================================================
def check_job_status():
    t = Timer(10, check_job_status)
    s1 = Session()
    rows = s1.query(Media).filter(Media.status == 'PENDING').all()
    for row in rows:
        t.start()
        r = requests.get('http://localhost:5000/api/v1/status/' + row.job_id)
        res = r.json()
        if res['state'] == 'SUCCESS':
            s2 = Session()
            media = s2.query(Media).filter(Media.id == row.id).one()
            media.status = res['state']
            media.updated_at = datetime.now()
            s2.commit()
            t.cancel()


# ============================================================================
# Processa o arquivo de texto inserido no sistema de arquivo, grava no banco
# de dados as informações relevantes, verifica a duração da mídia e envia para
# API de corte, aquela que possui duração maior que 30 segundos.
# ============================================================================
def process_file(filename, from_line=8):
    row = session.query(FileControl).filter(FileControl.id == session.query(func.max(FileControl.id))).first()
    if row and row.total_lines > 0:
        from_line = row.total_lines

    with open(filename, 'rb') as file:  
       line = file.readline()
       line_number = 1
       while line:
            if line_number > from_line:
                data = extract_fields(line)
                media = Media(**data)

                # Checa se a mídia possui mais de 30 segundos
                if media.duration_in_seconds() > 30:
                    # Envia mídia para recorte e recebe id, mensagem e status do job
                    payload = {
                        'start_time': data['start_time'],
                        'end_time': data['end_time'],
                        'title': data['title'],
                        'path': './processed',
                        'duration': data['duration'][0:8]
                    }
                    r = requests.post('http://localhost:5000/api/v1/cutmedia', json=payload)
                    res = r.json()
                    media.status = res['state']
                    media.job_id = res['job_id']
                else:
                    media.status = 'NOT_SENDED'
                media.created_at = datetime.now()
                session.add(media)
            line = file.readline()
            line_number += 1
    session.commit()
    control = FileControl()
    control.filename = filename
    control.total_lines = line_number
    control.created_at = datetime.now()
    session.add(control)
    session.commit()
    check_job_status()


if __name__ == '__main__':
    observer = Observer()
    observer.schedule(Handler(), path=DIR_MONITORED)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()