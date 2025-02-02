
import sys
import random
from PyQt5 import  QtCore
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtCore import *
from init import *
from agent import Mqtt_client
from icecream import ic
from datetime import datetime 

import logging

# Gets or creates a logger
logger = logging.getLogger(__name__)  

# set log level
logger.setLevel(logging.DEBUG)

# define file handler and set formatter
file_handler = logging.FileHandler('logfile_emulator.log')
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)


def time_format():
    return f'{datetime.now()}  Emulator|> '
ic.configureOutput(prefix=time_format)
ic.configureOutput(includeContext=False)
global clientname, tmp_upd

r=random.randrange(1,10000000)
clientname="IOT_clYT-Id-"+str(r)

class MC(Mqtt_client):
    def __init__(self):
        super().__init__()
    def on_message(self, client, userdata, msg):
            topic=msg.topic
            m_decode=str(msg.payload.decode("utf-8","ignore"))
            ic("message from:"+topic, m_decode)
            try:
                mainwin.connectionDock.update_btn_state(m_decode)
            except:
                ic("fail in update button state")

     
class ConnectionDock(QDockWidget):
    """Main """
    def __init__(self, mc, name, topic_sub, topic_pub):
        QDockWidget.__init__(self)        
        self.name = name
        self.topic_sub = topic_sub
        self.topic_pub = topic_pub
        self.mc = mc
        self.mc.set_on_connected_to_form(self.on_connected)
        self.eHostInput=QLineEdit()
        self.eHostInput.setInputMask('999.999.999.999')
        self.eHostInput.setText(broker_ip)        
        self.ePort=QLineEdit()
        self.ePort.setValidator(QIntValidator())
        self.ePort.setMaxLength(4)
        self.ePort.setText(broker_port)        
        self.eClientID=QLineEdit()
        global clientname
        self.eClientID.setText(clientname)        
        self.eUserName=QLineEdit()
        self.eUserName.setText(username)        
        self.ePassword=QLineEdit()
        self.ePassword.setEchoMode(QLineEdit.Password)
        self.ePassword.setText(password)        
        self.eKeepAlive=QLineEdit()
        self.eKeepAlive.setValidator(QIntValidator())
        self.eKeepAlive.setText("60")        
        self.eSSL=QCheckBox()        
        self.eCleanSession=QCheckBox()
        self.eCleanSession.setChecked(True)        
        self.eConnectbtn=QPushButton("Enable/Connect", self)
        self.eConnectbtn.setToolTip("click me to connect")
        self.eConnectbtn.clicked.connect(self.on_button_connect_click)
        self.eConnectbtn.setStyleSheet("background-color: gray")
        formLayot=QFormLayout()
        if 'Sensor' in self.name: 
            self.ePublisherTopic=QLineEdit()
            self.ePublisherTopic.setText(self.topic_pub)
            self.Temperature=QLineEdit()
            self.Temperature.setText('')            
            self.Humidity=QLineEdit()
            self.Humidity.setText('')                  
            formLayot.addRow("Turn On/Off",self.eConnectbtn)
            formLayot.addRow("Pub topic",self.ePublisherTopic)
            formLayot.addRow("Temperature",self.Temperature)
            formLayot.addRow("Humidity",self.Humidity)
        elif 'Air' in self.name:
            self.eSubscribeTopic=QLineEdit()
            self.eSubscribeTopic.setText(self.topic_sub)
            self.ePushtbtn=QPushButton("", self)
            self.ePushtbtn.setToolTip("Push me")
            self.ePushtbtn.setStyleSheet("background-color: gray")
            self.Temperature=QLineEdit()
            self.Temperature.setText('')   
            formLayot.addRow("Turn On/Off",self.eConnectbtn)
            formLayot.addRow("Sub topic",self.eSubscribeTopic)
            formLayot.addRow("Status",self.ePushtbtn)
            formLayot.addRow("Temperature",self.Temperature)        
        elif 'Elec' in self.name:
            self.ePublisherTopic=QLineEdit()
            self.ePublisherTopic.setText(self.topic_pub)
            self.Temperature=QLineEdit()
            self.Temperature.setText('')            
            self.Humidity=QLineEdit()
            self.Humidity.setText('')                  
            formLayot.addRow("Turn On/Off",self.eConnectbtn)
            formLayot.addRow("Pub topic",self.ePublisherTopic)
            formLayot.addRow("Electricity",self.Temperature)
            formLayot.addRow("Water",self.Humidity)
        else:
            self.eSubscribeTopic=QLineEdit()
            self.eSubscribeTopic.setText(self.topic_sub)
            self.ePushtbtn=QPushButton("", self)
            self.ePushtbtn.setToolTip("Push me")
            self.ePushtbtn.setStyleSheet("background-color: gray")
            self.Temperature=QLineEdit()
            self.Temperature.setText('')   
            formLayot.addRow("Turn On/Off",self.eConnectbtn)
            formLayot.addRow("Sub topic",self.eSubscribeTopic)
            formLayot.addRow("Status",self.ePushtbtn)
            formLayot.addRow("Temperature",self.Temperature)
        widget = QWidget(self)
        widget.setLayout(formLayot)
        self.setTitleBarWidget(widget)
        self.setWidget(widget)     
        self.setWindowTitle("IOT Emulator")        
    def on_connected(self):
        self.eConnectbtn.setStyleSheet("background-color: green")                    
    def on_button_connect_click(self):
        self.mc.set_broker(self.eHostInput.text())
        self.mc.set_port(int(self.ePort.text()))
        self.mc.set_clientName(self.eClientID.text())
        self.mc.set_username(self.eUserName.text())
        self.mc.set_password(self.ePassword.text())        
        self.mc.connect_to()        
        self.mc.start_listening()
    def push_button_click(self):
        self.mc.publish_to(self.ePublisherTopic.text(), '"value":1')
    def update_btn_state(self,messg):
        global tmp_upd
        if 'Set' in messg:
            tmp='12'
            self.ePushtbtn.setStyleSheet("background-color: green")            
            try:
                tmp=messg.split('Set temperature to: ')[1]
                self.Temperature.setText(tmp)
            except:
                ic("fail in parsing temperature !!!!!!!!!!!!!!!!")            
            tmp_upd=tmp


class MainWindow(QMainWindow):    
    def __init__(self, args, parent=None):
        QMainWindow.__init__(self, parent)        
        # Parse sys arg values
        global tmp_upd
        
        self.name = args[1]
        self.units = args[2]
        self.topic_sub = comm_topic+args[3]+'/sub'
        self.topic_pub = comm_topic+args[3]+'/pub'
        self.update_rate = args[4]
        # Init of Mqtt_client class        
        self.mc=MC()
        if 'Sensor' in self.name:
            tmp_upd = 22
            # Creating timer for update rate support
            self.timer = QtCore.QTimer(self)
            self.timer.timeout.connect(self.create_data_Sensor1)
            self.timer.start(int(self.update_rate)*1000) # in msec    

        elif 'Meter' in self.name: 
            # Creating timer for update rate support
            self.timer = QtCore.QTimer(self)
            self.timer.timeout.connect(self.create_data_EW)
            self.timer.start(int(self.update_rate)*1000) # in msec        
        elif 'Airconditioner' in self.name:          
            # Creating timer for update rate support
            self.timer = QtCore.QTimer(self)
            self.timer.timeout.connect(self.create_data_Air)
            self.timer.start(int(self.update_rate)*1000) # in msec

        # general GUI settings
        self.setUnifiedTitleAndToolBarOnMac(True)
        # set up main window
        self.setGeometry(30, 600, 300, 150)
        self.setWindowTitle(self.name)
        # Init QDockWidget objects        
        self.connectionDock = ConnectionDock(self.mc, self.name, self.topic_sub, self.topic_pub)       
        self.addDockWidget(Qt.TopDockWidgetArea, self.connectionDock)        

    def create_data(self):
        global tmp_upd
        ic('Next update')
        temp=tmp_upd+random.randrange(1,6)
        hum=66+random.randrange(1,10)
        current_data= 'From: ' + self.name+ ' Temperature: '+str(temp)+' Humidity: '+str(hum)
        self.connectionDock.Temperature.setText(str(temp))
        self.connectionDock.Humidity.setText(str(hum))
        if not self.mc.connected:
            self.connectionDock.on_button_connect_click()
        self.mc.publish_to(self.topic_pub,current_data)

    def create_data_EW(self):
        ic('Electricity-Water data update')
        hour_delta_w = 0.42/24
        hour_delta_el = (670/17)/24
        elec= format(hour_delta_el+random.randrange(-100,100)/300, '.2f') 
        water=format(hour_delta_w +random.randrange(-10,10)/1000, '.3f')
        current_data= 'From: ' + self.name + ' Electricity: '+str(elec)+' Water: '+str(water)
        self.connectionDock.Temperature.setText(str(elec))
        self.connectionDock.Humidity.setText(str(water))
        if not self.mc.connected:
            self.connectionDock.on_button_connect_click()
        self.mc.publish_to(self.topic_pub,current_data)

    def create_data_Air(self):
        ic('Sensor data update')        
        if not self.mc.connected:
            self.connectionDock.on_button_connect_click()
        if not self.mc.subscribed:
            self.mc.subscribe_to(self.topic_sub)

    def create_data_Sensor1(self):
        global tmp_upd
        
        ic('Sensor 1 data update')        
        if not self.mc.connected:
            self.connectionDock.on_button_connect_click()
        if not self.mc.subscribed:
            self.mc.subscribe_to(self.topic_sub)
        temp=tmp_upd+random.randrange(-10,-5)/10        
        current_data=  'Temperature: '+str(temp)
        
        self.connectionDock.Temperature.setText(str(temp))  
        hum=67+random.randrange(1,10) 
        self.connectionDock.Humidity.setText(str(hum))     
        self.mc.publish_to(self.topic_pub,current_data)

    def create_data_Sensor2(self):
        global tmp_upd
        ic('Sensor 2 data update')        
        if not self.mc.connected:
            self.connectionDock.on_button_connect_click()
        if not self.mc.subscribed:
            self.mc.subscribe_to(self.topic_sub)
        temp=tmp_upd+random.randrange(-10,-5)/10        
        current_data=  'Temperature: '+str(temp)
        self.connectionDock.Temperature.setText(str(temp))        
        hum=67+random.randrange(1,10) 
        self.connectionDock.Humidity.setText(str(hum))     
        self.mc.publish_to(self.topic_pub,current_data)    

    def create_data_Sensor3(self):
        global tmp_upd
        ic('Sensor 3 data update')        
        if not self.mc.connected:
            self.connectionDock.on_button_connect_click()
        if not self.mc.subscribed:
            self.mc.subscribe_to(self.topic_sub)
        temp=tmp_upd+random.randrange(1,20)/2        
        current_data=  'Temperature: '+str(temp)
        hum=68+random.randrange(1,10) 
        self.connectionDock.Humidity.setText(str(hum))     
        self.connectionDock.Temperature.setText(str(temp))       
        self.mc.publish_to(self.topic_pub,current_data)

if __name__ == '__main__':

    try:    
        app = QApplication(sys.argv)
        argv=sys.argv
        if len(sys.argv)==1:
            argv.append('Sensor')
            argv.append('Celsius')
            argv.append('air-1')
            argv.append('7')

        mainwin = MainWindow(argv)
        mainwin.show()
        app.exec_()
    except:
        logger.exception("Crash!")
