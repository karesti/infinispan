@echo off

setlocal enabledelayedexpansion
set ISPN_HOME=%~dp0
set ISPN_HOME="%ISPN_HOME%.."

set CP=
for %%i in (%ISPN_HOME%\*.jar) do call :append_to_cp %%i
rem echo libs: %CP%

java -classpath "%CP%" org.infinispan.cli.Main %*

goto :eof

:append_to_cp
set CP=%CP%;%1
goto :eof
