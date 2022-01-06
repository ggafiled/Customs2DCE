import sys
import os
import datetime
import time
import pandas as pd
from threading import Thread
from PyQt5.QtWidgets import QWidget, QApplication, QMainWindow, QDialog, QFileDialog, QMessageBox, QErrorMessage
from PyQt5 import QtCore, QtGui, uic
from PyQt5.QtGui import QIntValidator
from concurrent.futures import Future

txtSourcePath = ""
txtDestinationPath = ""


def call_with_future(fn, future, args, kwargs):
    try:
        result = fn(*args, **kwargs)
        future.set_result(result)
    except Exception as exc:
        future.set_exception(exc)

def threaded(fn):
    def wrapper(*args, **kwargs):
        future = Future()
        Thread(target=call_with_future, args=(fn, future, args, kwargs)).start()
        return future
    return wrapper

class UI(QMainWindow):
    def __init__(self):
        super(UI, self).__init__()
        uic.loadUi(os.path.dirname(__file__) + '/resource/main.ui', self)
        self.setWindowIcon(QtGui.QIcon(os.path.dirname(__file__) +  '/resource/logo.png'))
        self.btnSourceSelect.clicked.connect(self.choose_source_data)
        self.btnDestinationSelect.clicked.connect(self.choose_destination_folder)
        self.chbUseSplit.stateChanged.connect(self.state_changed)
        self.buttonBox.accepted.connect(self.do_process)
        self.buttonBox.rejected.connect(self.close)
        self.onloaded()
        self.show()

    def onloaded(self):
        self.progressBar.hide()
        self.txtBoxCountOfSplit.setEnabled(False)
        self.txtBoxCountOfSplit.setText("100000")
        self.txtBoxCountOfSplit.setValidator(QIntValidator())
        self.txtSourcePath.setText("")
        self.txtDestinationPath.setText("")
        self.txtSourcePath.setReadOnly(True)
        self.txtDestinationPath.setReadOnly(True)
        self.chbUseSplit.setChecked(False)

    def choose_source_data(self):
        fileName, _ = QFileDialog.getOpenFileName(self,"QFileDialog.getOpenFileName()", "","CSV (*.csv)")
        if fileName:
            self.txtSourcePath.setText(fileName)
            
    def choose_destination_folder(self):
        directory_path = str(QFileDialog.getExistingDirectory(self, "Select Directory"))
        if directory_path:
            self.txtDestinationPath.setText(directory_path)

    def do_process(self):
        global txtSourcePath, txtDestinationPath
        txtSourcePath = self.txtSourcePath.text()
        txtDestinationPath = self.txtDestinationPath.text()

        if not txtSourcePath or not txtDestinationPath:
            msg = QMessageBox.about(self, "โปรดทราบ", "กรุณาระบุที่อยู่ไฟล์ตั้งต้น หรือ ที่อยู่ไฟล์สำหรับจัดเก็บข้อมูลให้ครบถ้วนค่ะ")
            return

        self.progressBar.show()
        try:
            self.progressBar.setValue(50)
            future_result = self.convert_to_dce_format()
            self.progressBar.setValue(60)
            result = future_result.result()
            self.progressBar.setValue(70)
            if not os.path.exists(txtDestinationPath):
                os.mkdir(txtDestinationPath)
            self.progressBar.setValue(80)
            time.sleep(1)
            if not self.chbUseSplit.isChecked():
                result.to_csv(os.path.join(self.txtDestinationPath.text(),f"{datetime.date.today().strftime('%Y-%m-%d')}_TH-Tariff-HScode.csv"), index=False)
                self.progressBar.setValue(100)
            else:
                self.progressBar.setValue(85)
                self.split_items(result)
                self.progressBar.setValue(100)
            msg = QMessageBox.about(self, "เสร็จสิ้น", "ดำเนินการเสร็จสิ้น")

        except Exception as e:
            msg = QMessageBox.about(self, "มีบางอย่างผิดปกติ", str(e))
            
        time.sleep(2)
        self.onloaded()

    def close(self):
        sys.exit(app.exec_())

    @threaded
    def convert_to_dce_format(self):
        global txtSourcePath, txtDestinationPath
        try:
            if os.path.exists(txtSourcePath):
                export_df = pd.DataFrame()
                import_df = pd.DataFrame()

                current_year = datetime.date.today()
                first_day_of_year = datetime.date.min.replace(year = current_year.year)
                first_day_of_last_year = first_day_of_year.replace(first_day_of_year.year + 5)
                import_df = pd.read_csv(txtSourcePath, sep=',', converters={i: str for i in range(1)})
                import_df.PERCENT = import_df.PERCENT.str.rstrip('%').astype('float') / 100.0
                export_df["HS Code"] = import_df.TARIFF
                export_df["Tariff Type"] = "TARIFF"
                export_df["Part Number"] = ""
                export_df["Related HS Code"] = ""
                export_df["CALC Modifier"] = ""
                export_df["Description"] = import_df.DES
                export_df["Local Description"] = ""
                export_df["Notes 1"] = ""
                export_df["Notes 2"] = ""
                export_df["Unit of Measure"] = "KGM"
                export_df["Country Groups"] = ""
                export_df["Country Exemptions"] = ""
                export_df["Tariff Value"] = import_df.PERCENT
                export_df["Tariff UOM Value"] = 0
                export_df["Start Date"] = first_day_of_year.strftime("%d/%m/%Y")
                export_df["End Date"] = first_day_of_last_year.strftime("%d/%m/%Y")
            
            return export_df
        except Exception as e:
            raise e
            

    def split_items(self, list):
        global txtDestinationPath
        sizeOfList = len(list)
        splitSize = int(self.txtBoxCountOfSplit.text())
        if sizeOfList  <= splitSize:
            list.to_csv(os.path.join(txtDestinationPath,f"{datetime.date.today().strftime('%Y-%m-%d')}_TH-Tariff-HScode.csv"), index=False)
        else:
            roundCount = round(sizeOfList / splitSize)
            for i in range(roundCount):
                df = list[splitSize*i:splitSize*(i+1)]
                df.to_csv(os.path.join(txtDestinationPath,f"{datetime.date.today().strftime('%Y-%m-%d')}_TH-Tariff-HScode_Part_{i+1}.csv"), index=False)

    def state_changed(self):
        if self.chbUseSplit.isChecked():
            self.txtBoxCountOfSplit.setEnabled(True)
        else:
            self.txtBoxCountOfSplit.setEnabled(False)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = UI()
    sys.exit(app.exec_())