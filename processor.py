import datetime, csv, io
from mysql import connector

    
def integrate(x,y):
    area = 0
    for x0,_y,x1 in zip(x[:-1], y[:-1], x[1:]):
        area += (x1-x0) * _y
    return area


def tickhour_average_kW(time, values):
    hours_y = {}
    for i,j in zip(time, values):
        index = i // 3600#datetime.datetime.strptime(i, "%d.%m.%Y %H:%M:%S").hour
        hours_y[index] = hours_y.get(index, []) + [j]
        
    out_x = []
    out_y = []
    for i in hours_y:
        out_y += [sum(hours_y[i]) / len(hours_y[i])]
        out_x += [i]
        
    return (out_x, out_y)


def getStartTime(T):
    return datetime.datetime(int(T[6:10]), int(T[3:5]), int(T[:2]), 0, 0, 0)

def parseTime(T):
    # return T
    return int(T[11:13]) * 3600 \
        + int(T[14:16]) * 60 \
        + int(T[17:])

def parseFloat(F):
    return float(F.replace(",", "."))

def processCSV(path, time_column, data_column):
    with open(path) as f:
        D = f.read().splitlines()[1:]
        data = io.StringIO('\n'.join(D))
        lt = datetime.datetime.strptime(D[-1].split(";")[0], "%d.%m.%Y %H:%M:%S")
        del D

        print(lt)

        ignore_before = getStartTime("01.01.2020 13:40:00") # get last entry in day table 
        reader = csv.DictReader(data, delimiter=";")

        times_day = []
        data_day = []

        last = ignore_before.strftime("%d.%m.%Y")
        yesterday = (lt - datetime.timedelta(days=1)).strftime("%d.%m.%Y")

        mdata = {}
        

        yesterday_data = None

        hist = []

        for row in reader:
            tme = datetime.datetime.strptime(row[time_column], "%d.%m.%Y %H:%M:%S")
            if tme < ignore_before:
                continue
            if tme.strftime("%d.%m.%Y")==last:
                times_day.append(parseTime(row[time_column]))
                data_day.append(abs(parseFloat(row[data_column])))
            else:
                day = 1/3600 * integrate(times_day, data_day)
                last = tme.strftime("%d.%m.%Y")
                mdata[last[3:]] = mdata.get(last[3:], 0) + day
                hist.append(
                    (
                        (tme -datetime.timedelta(days=1)).strftime("%d.%m.%Y"), 
                        day
                    )
                )
                
                if last == yesterday:
                    yesterday_data = (times_day, data_day, last)
                times_day = []
                data_day = []
 

        out_yesterday = None
        if yesterday_data:
            out_yesterday = tickhour_average_kW(yesterday_data[0], yesterday_data[1])

        out = tickhour_average_kW(times_day, data_day)
        
        return (out, out_yesterday, (mdata.keys(), mdata.values()), yesterday_data[2], hist)



def upload_yesterday(data, last, host, username, password, databaseName):
    conn = connector.connect(
        host=host,
        user=username,
        password=password,
        database=databaseName,
    )
    date = '-'.join(last.split(".")[::-1])
    cursor = conn.cursor()
    cursor.execute(f"SELECT record_time FROM `yesterday:production` WHERE record_time > '{date} 00:00:00';")
    upload = None
    for i in cursor.fetchall():
        upload = i[0].hour
    
    for i,j in zip(data[0], data[1]):
        curdate = date + f" {'0' if i<10 else ''}{i}:00:00"
        if upload:
            if i<upload:
                continue
            if i==upload:
                cursor.execute(f"UPDATE `yesterday:production` SET `averageKW` = {j} WHERE `record_time`='{curdate}';")
                continue
        cursor.execute(f"INSERT INTO `yesterday:production` (`record_time`, `averageKW`) VALUES ('{curdate}', {j});")
    conn.commit()
    conn.close()

def upload_KWH(data, host, username, password, databaseName):
    conn = connector.connect(
        host=host,
        user=username,
        password=password,
        database=databaseName,
    )
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(record_time) FROM `full:production`;")
    upload = None
    try:
        for i in cursor.fetchall():
            upload = i[0]
    except AttributeError: pass
    for i,j in data:
        dte = "-".join(i.split(".")[::-1])
        if upload:
            date = datetime.datetime.strptime(i, "%d.%m.%Y")
            if date < upload:
                continue
            if date == upload:
                cursor.execute(F"UPDATE `full:production` SET `kWh` = {j} WHERE `record_time`='{dte}';")
                continue
        cursor.execute(f"INSERT INTO `full:production` (`record_time`, `kWh`) VALUES ('{dte}', {j});")
    conn.commit()
    conn.close()

