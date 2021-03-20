import os
import io
import re   
import csv
import time
import json  
import zipfile, logging
import datetime
import threading
import subprocess
import openEnvis
from pywinauto.application import Application
import mysql.connector as connector
import processor


host = "host"
username = "username"
password = "password"
databaseName = "databaseName"


logger = logging.getLogger("filesys.py")
handler = logging.FileHandler(r"path/to/logfile")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("[%(asctime)s] %(name)s:%(levelname)s:%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def move(source_folder, destination_folder, filename):
    os.rename(source_folder+"\\"+filename, destination_folder+"\\"+filename)

    return destination_folder+"\\"+filename


class Listener(threading.Thread):

    def __init__(self, name, ip, download_directory, deposit_directory, error_directory, temp_directory):
        threading.Thread.__init__(self)
        self.ip = ip
        self.download_directory = download_directory
        self.deposit_directory = deposit_directory
        self.name = name
        self.error_directory = error_directory
        self.tempdir = temp_directory
        self.dead = False

    def find_new_files(self):
        fls = []
        dirs = []

        for path, directories, files in os.walk(self.download_directory):
            if not files and not directories:
                return None
            fls.extend([i for i in files])
            dirs.extend([name for name in directories])

        return fls, dirs

    def createTable(self, tablename, filename, database):
        logger.info(f"creating {tablename}")
        with open(filename) as f:
            data = io.StringIO('\n'.join(f.read().splitlines()[1:]))
            reader = csv.DictReader(data, delimiter=";")
            for i in reader:
                args = []
                for key in i:
                    value = i[key]
                    if re.match(r"[0-9][0-9]\.[0-9][0-9]\.[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]", value):   # date
                        args.append(f"`{key}` DATETIME")
                    elif re.match(r"-?[0-9]+(,[0-9]+)?", value): # float
                        args.append(f"`{key}` FLOAT")
                    elif re.match(r"ok|true|false|10mtick", value.casefold()): # bool
                        args.append(f"`{key}` BOOLEAN")
                
                command = f"CREATE TABLE IF NOT EXISTS `{tablename}` (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, {', '.join(args)});"
                cursor = database.cursor()
                cursor.execute(command)
                database.commit()
                break

    def process_csv(self, filename, time_row):
        fname, ext = os.path.splitext(os.path.basename(filename))
        try:
            db = connector.connect(
                host=host,
                user=username,
                password=password,
                database=databaseName,
            )
            cursor = db.cursor()
            columns = []
            try:
                cmd = f"SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{databaseName}' AND TABLE_NAME = '{self.name}:{fname}';"
                cursor.execute(cmd)
                res = cursor.fetchall()
                if len(res)==0:
                    self.createTable(f"{self.name}:{fname}", filename, db)
                    last_time = None
                cursor.execute(f"SHOW COLUMNS FROM `{self.name}:{fname}`;")
                columns = [i[0] for i in cursor.fetchall()]
                cursor.execute(f"SELECT MAX(`{time_row}`) FROM `{self.name}:{fname}`;")
                last_time = cursor.fetchall()[0][0].timestamp()
            except Exception as e:
                last_time = None
            with open(filename) as f:
                data = io.StringIO('\n'.join(f.read().splitlines()[1:]))
                reader = csv.DictReader(data, delimiter=";")
                lines_written = 0
                for row in reader:
                    values = []
                    header = []
                    if last_time:
                        dt = datetime.datetime.strptime(
                            row[time_row], "%d.%m.%Y %H:%M:%S")
                        now = dt.timestamp()
                        if now <= last_time:
                            continue
                    for i in row:
                        if i not in columns:
                            continue
                        val = row[i].lower()
                        if re.match(r"[0-9][0-9]\.[0-9][0-9]\.[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]", val) and not re.search(r"[a-zA-Z]", val):  # datetime
                            _date = val.split(" ")[0].split(".")
                            _time = val.split(" ")[1]
                            _date.reverse()
                            val = f'"{"-".join(_date)} {_time}"'
                        elif re.match(r"-?[0-9]+(,[0-9]+)?", val) and not re.search(r"[a-z]", val):  # float
                            val = val.replace(",", ".")
                        elif re.match(r"ok|true|false|10mtick", val.casefold()):  # bool
                            if re.search(r"ok|true", val.casefold()):
                                val = "1"
                            else:
                                val = "0"
                        else:
                            continue

                        if val=="10mtick":
                            #print("10mtick")
                            val = "0"

                        header.append(f"`{i}`")
                        values.append(val)
                    cmd = f"INSERT INTO `{self.name}:{fname}` ({', '.join(header[:-2])}) \n VALUES ({', '.join(values[:-2])});"
                    cursor.execute(cmd)
                    lines_written += 1
                db.commit()
                logger.info(f"{self.name}:{fname} has been written ({lines_written} lines) ")
                cursor.execute(f"INSERT INTO `logs` (time, code, message, ip) VALUES (NOW(), 0, \"success {lines_written} rows\", \"{self.ip}\" );")
                db.commit()
        except Exception as e:
            print(e)
            logger.info(f"{e} ") # info instead of warning, may need to properly setup logger
            return 1
        return 0

    def run(self):
        while not self.dead:
            out = self.find_new_files()
            if out:
                self.dead = True
            time.sleep(5)



class Processor:
    def __init__(self, name, ip, download_directory, deposit_directory, error_directory, temp_directory) -> None:
        self.ip = ip
        self.download_directory = download_directory
        self.deposit_directory = deposit_directory
        self.name = name
        self.error_directory = error_directory
        self.tempdir = temp_directory

    def find_new_files(self):
        fls = []
        dirs = []

        for path, directories, files in os.walk(self.download_directory):
            if not files and not directories:
                return None
            fls.extend([i for i in files])
            dirs.extend([name for name in directories])

        return fls, dirs

    def createTable(self, tablename, filename, database):
        logger.info(f"creating {tablename}")
        with open(filename) as f:
            data = io.StringIO('\n'.join(f.read().splitlines()[1:]))
            reader = csv.DictReader(data, delimiter=";")
            for i in reader:
                args = []
                for key in i:
                    value = i[key]
                    if re.match(r"[0-9][0-9]\.[0-9][0-9]\.[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]", value):   # date
                        args.append(f"`{key}` DATETIME")
                    elif re.match(r"-?[0-9]+(,[0-9]+)?", value): # float
                        args.append(f"`{key}` FLOAT")
                    elif re.match(r"ok|true|false|10mtick", value.casefold()): # bool
                        args.append(f"`{key}` BOOLEAN")
                
                command = f"CREATE TABLE IF NOT EXISTS `{tablename}` (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, {', '.join(args)});"
                cursor = database.cursor()
                cursor.execute(command)
                database.commit()
                break

    def process_csv(self, filename, name, time_column, data_column):
        try:
            if re.search(r"Hauptarchiv.csv", filename):
                day, yesterday, year, last, data = processor.processCSV(filename, time_column, data_column)
                processor.upload_KWH(data, host, username, password, databaseName)
                processor.upload_yesterday(yesterday, last, host, username, password, databaseName)
            return 0
        except Exception as e:
            return 1


    def __process_csv(self, filename, time_row):
        fname, ext = os.path.splitext(os.path.basename(filename))
        try:
            db = connector.connect(
                host=host,
                user=username,
                password=password,
                database=databaseName,
            )
            cursor = db.cursor()
            columns = []
            try:
                cmd = f"SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{databaseName}' AND TABLE_NAME = '{self.name}:{fname}';"
                cursor.execute(cmd)
                res = cursor.fetchall()
                if len(res)==0:
                    self.createTable(f"{self.name}:{fname}", filename, db)
                    last_time = None
                cursor.execute(f"SHOW COLUMNS FROM `{self.name}:{fname}`;")
                columns = [i[0] for i in cursor.fetchall()]
                cursor.execute(f"SELECT MAX(`{time_row}`) FROM `{self.name}:{fname}`;")
                last_time = cursor.fetchall()[0][0].timestamp()
            except Exception as e:
                last_time = None
            with open(filename) as f:
                data = io.StringIO('\n'.join(f.read().splitlines()[1:]))
                reader = csv.DictReader(data, delimiter=";")
                lines_written = 0
                for row in reader:
                    values = []
                    header = []
                    if last_time:
                        dt = datetime.datetime.strptime(
                            row[time_row], "%d.%m.%Y %H:%M:%S")
                        now = dt.timestamp()
                        if now <= last_time:
                            continue
                    for i in row:
                        if i not in columns:
                            continue
                        val = row[i].lower()
                        if re.match(r"[0-9][0-9]\.[0-9][0-9]\.[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]", val) and not re.search(r"[a-zA-Z]", val):  # datetime
                            _date = val.split(" ")[0].split(".")
                            _time = val.split(" ")[1]
                            _date.reverse()
                            val = f'"{"-".join(_date)} {_time}"'
                        elif re.match(r"-?[0-9]+(,[0-9]+)?", val) and not re.search(r"[a-z]", val):  # float
                            val = val.replace(",", ".")
                        elif re.match(r"ok|true|false|10mtick", val.casefold()):  # bool
                            if re.search(r"ok|true", val.casefold()):
                                val = "1"
                            else:
                                val = "0"
                        else:
                            continue

                        if val=="10mtick":
                            val = "0"

                        header.append(f"`{i}`")
                        values.append(val)
                    cmd = f"INSERT INTO `{self.name}:{fname}` ({', '.join(header[:-2])}) \n VALUES ({', '.join(values[:-2])});"
                    cursor.execute(cmd)
                    lines_written += 1
                    if lines_written % 5000==0:
                        print(f'{self.name}:{fname} : [{lines_written//5000}] {"#"*int(lines_written//5000)}')
                        db.commit()
                db.commit()
                logger.info(f"{self.name}:{fname} has been written ({lines_written} lines) ")
                cursor.execute(f"INSERT INTO `logs` (time, code, message, ip) VALUES (NOW(), 0, \"success {lines_written} rows\", \"{self.ip}\" );")
                db.commit()
        except Exception as e:
            print(e)
            logger.info(f"{e} ") # info instead of warning, may need to properly setup logger
            return 1
        return 0

    def process(self):
        out = self.find_new_files()
        if out:
            for i in out[0]:
                try:
                    with zipfile.ZipFile(self.download_directory + "\\" + i) as zf:
                        zf.extractall(self.tempdir)
                    fls = []
                    for path, directories, files in os.walk(self.tempdir):
                        fls.extend([self.tempdir + "\\" + k for k in files])
                    statuscode = 0
                    for j in fls:
                        if re.search(r"Hauptarchiv", j):
                            print(j)
                            # change 'name' to whatever is applicable in future
                            statuscode = self.process_csv(j, "main", "Datensatzzeit[s]", "Durchschnitt.3P[kW]") if statuscode==0 else 1
                    if statuscode==0:
                        move(self.download_directory, self.deposit_directory, i)
                    else:
                        move(self.download_directory, self.error_directory, i)
                    for path, directories, files in os.walk(self.tempdir):
                        for i in path:
                            os.remove(os.path.join(path, i))
                except Exception as e:
                    print(e)
                    move(self.download_directory, self.error_directory, i)
                    logger.warning(f"{i} could not be extracted")


class Handler:
    def __init__(self, tasks):
        self.tasks = tasks

    def download(self, app):
        for task in self.tasks:
            try:
                openEnvis.openEnvis(task["ip"], task["download_directory"], task["name"]+datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S"), app)
            except Exception as e:
                logger.info(f"error '{e}' while trying to open {task['name']}")

    def single(self, taskname, app):
        task = None
        for i in self.tasks:
            if i["name"]==taskname:
                task = i
                break
        assert task is not None
        
        openEnvis.openEnvis(task["ip"], task["download_directory"], task["name"]+datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S"), app)    
        
        print("opened ", task["ip"])
        thread = Listener(
                task["name"],
                task["ip"],
                task["download_directory"],
                task["deposit_directory"],
                task["error_depository"],
                task["temp_directory"]
            )
        thread.start()
        thread.join()


    def startListeners(self):
        threads = []
        for task in self.tasks:
            threads.append(Listener(
                task["name"],
                task["ip"],
                task["download_directory"],
                task["deposit_directory"],
                task["error_depository"],
                task["temp_directory"]
            ))
        for thread in threads:
            thread.start()
        allDone = False
        while not allDone:
            allDone = True
            for thread in threads:
                if thread.is_alive():
                    allDone = False
        
    def upload(self):
        for task in self.tasks:
            uploader = Processor(
                task["name"],
                task["ip"],
                task["download_directory"],
                task["deposit_directory"],
                task["error_depository"],
                task["temp_directory"]
            )
            uploader.process()

    def cleanup(self):
        for task in self.tasks:
            uploader = Processor(
                task["name"],
                task["ip"],
                task["download_directory"],
                task["deposit_directory"],
                task["error_depository"],
                task["temp_directory"]
            )
            if uploader.find_new_files():
                print(f"{task['name']} has new file(s)")
                uploader.process()
        

    def main(self, min_delay):
        while True:
            app = Application(backend="uia")
            last = time.time()
            self.download(app)
            self.startListeners()
            app.kill()
            self.upload()
            print("uploaded")
            t = time.time()
            if t - last < min_delay:
                print(t - last)
                time.sleep(last + min_delay - t)

    def singles(self, min_delay):
        while True:
            print("start")
            last = time.time()
            app = Application(backend="uia")
            for task in tasks:
                self.single(task["name"], app)
                logger.log(logging.INFO, f"downloaded {task['name']}")
            t = time.time()
            app.kill()
            if t - last < min_delay:
                print(t - last)
                time.sleep(last + min_delay - t)
            subprocess.call(["taskkill", "/IM", "ENVIS.Daq.exe"])
                
        


if __name__ == "__main__":
    tasks = []
    with open(r"path/to/config") as f:
        data = json.load(f)
        tasks = data["tasks"]
    print(f"loaded tasks: {len(tasks)}")
    dataHandler = Handler(tasks)
    print("created handler")
    dataHandler.cleanup()
    print("cleaned directory")
    dataHandler.main(60*60*2)