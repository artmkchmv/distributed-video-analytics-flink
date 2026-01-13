@echo off
chcp 65001 >nul

echo Starting Applications

if not exist "scaler\target\scaler-1.0.0.jar" (
    echo Error: scaler\target\scaler-1.0.0.jar not found. Build project: mvn clean package
    pause
    exit /b 1
)
if not exist "producer\target\producer-1.0.0.jar" (
    echo Error: producer\target\producer-1.0.0.jar not found. Build project: mvn clean package
    pause
    exit /b 1
)
if not exist "processor\target\processor-1.0.0.jar" (
    echo Error: processor\target\processor-1.0.0.jar not found. Build project: mvn clean package
    pause
    exit /b 1
)

if not exist "videos\processed-data" mkdir "videos\processed-data"
if not exist "flink-checkpoint" mkdir "flink-checkpoint"

echo.
echo Starting Scaler Node...
start "Scaler Node" cmd /k "java -jar scaler\target\scaler-1.0.0.jar"

timeout /t 3 /nobreak >nul

echo Starting Producer Node...
start "Producer Node" cmd /k "java -jar producer\target\producer-1.0.0.jar"

timeout /t 3 /nobreak >nul

echo Starting Processor Node (Flink job)...
start "Processor Node" cmd /k "java --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED -jar processor\target\processor-1.0.0.jar"

echo.
echo All applications started in separate windows
echo Check logs in each window
pause
