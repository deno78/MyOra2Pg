@echo off
setlocal enabledelayedexpansion
rem MavenリポジトリのベースURL
set BASE_URL=https://repo1.maven.org/maven2

rem Tools/libフォルダのパス
set LIB_DIR=%~dp0lib

mkdir %LIB_DIR%

for %%f in (
com/oracle/database/jdbc/ojdbc8/23.6.0.24.10/ojdbc8-23.6.0.24.10.jar 
org/codehaus/groovy/groovy-all/2.4.21/groovy-all-2.4.21.jar 
org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar 
) do (
    set "JAR_PATH=%%f"
    set "FILENAME=%%~nxf"
    bitsadmin /TRANSFER "myDownloadJob" /download /priority normal !BASE_URL!/!JAR_PATH! !LIB_DIR!\!FILENAME!
)

echo All jars have been downloaded.
pause

