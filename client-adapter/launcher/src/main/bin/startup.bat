@echo off
@if not "%ECHO%" == ""  echo %ECHO%
@if "%OS%" == "Windows_NT"  setlocal

set ENV_PATH=.\
if "%OS%" == "Windows_NT" set ENV_PATH=%~dp0%

set conf_dir=%ENV_PATH%\..\conf

set CLASSPATH=%conf_dir%
set CLASSPATH=%conf_dir%\..\lib\*;%CLASSPATH%

set JAVA_MEM_OPTS= -Xms128m -Xmx512m -XX:PermSize=128m
set JAVA_OPTS_EXT= -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dapplication.codeset=UTF-8 -Dfile.encoding=UTF-8
set ADAPTER_OPTS= -DappName=canal-adapter

set JAVA_OPTS= %JAVA_MEM_OPTS% %JAVA_OPTS_EXT% %ADAPTER_OPTS%

set CMD_STR= java %JAVA_OPTS% -classpath "%CLASSPATH%" com.alibaba.otter.canal.adapter.launcher.CanalAdapterApplication
echo start cmd : %CMD_STR%

java %JAVA_OPTS% -classpath "%CLASSPATH%" com.alibaba.otter.canal.adapter.launcher.CanalAdapterApplication