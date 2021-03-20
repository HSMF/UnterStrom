import pywinauto 
import time
from pywinauto.application import Application
from pywinauto.keyboard import send_keys

envis_path = r"path/to/envis.exe"

time.sleep(1)

def openEnvis(ip, path, fname, app):
    app.start(envis_path)

    dlg_ = app.window(best_match = "ENVIS.Daq")
    d = dlg_.wait("visible")

    send_keys("{TAB}{TAB}{TAB}{TAB}")
    time.sleep(0.1)
    send_keys(ip)
    time.sleep(0.1)
    send_keys("{ENTER}")

    dlwindow = app.window(best_match="DEFAULT/DEFAULT")
    dwindow = dlwindow.wait("exists")

    pane = dlwindow.child_window(control_type="Pane", title="pan_main")
    pane.wait("exists")
    navBarControl2 =  pane.window(best_match="navBarControl2")
    abtDAQ0 = navBarControl2.window(best_match="über Datenerfassung (DAQ)")
    abtDAQ1 = abtDAQ0.window(best_match="über Datenerfassung (DAQ)")
    abtDAQ2 = abtDAQ1.window(best_match="über Datenerfassung (DAQ)")
    panelControl2 = abtDAQ2.window(best_match="panelControl2")
    table = panelControl2.window(best_match='Archives to downloadTable')
    DataPanel = table.window(best_match="Datenbereich")
    Row8 = DataPanel.window(best_match="Zeile 8")

    refresh = Row8.window(best_match="zählen row 7")

    refresh.click_input()

    time.sleep(1)

    download = Row8.window(best_match="herunterladen row 7")
    download.click_input()

    time.sleep(1)

    send_keys(path + "\\"+ fname)
    send_keys("{ENTER}")

    dlwindow.minimize()
