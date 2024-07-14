start "Sensor 1" python emulator.py Sensor1 Celsius Room_1 7  
timeout 3 
start "Sensor 2" python emulator.py Sensor2 Celsius Common 11
timeout 3
start "Sensor 3" python emulator.py Sensor3 Celsius Common 11
timeout 3
start "Emulator: Electricity&Water Meter" python emulator.py ElecWaterMeter kWh Home 13
timeout 3
@REM start "Emulator: Airconditioner" python emulator.py Airconditioner Celsius air-1 5
@REM timeout 3
@REM start "Emulator: Freezer" python emulator.py Freezer Celsius freezer 6
@REM timeout 3
@REM start "Emulator: Boiler" python emulator.py Boiler Celsius boiler 8
@REM timeout 3
@REM start "Emulator: Refrigerator" python emulator.py Refrigerator Celsius refrigerator 9
@REM timeout 3
@REM start "Smart Home Manager" python manager.py
timeout 10
start "System GUI" python gui.py