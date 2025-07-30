@echo off

set JAVA_HOME=C:\Program Files\ojdkbuild\java-17-openjdk-17.0.3.0.6-1
set PATH=%JAVA_HOME%\bin;%PATH%

setlocal enabledelayedexpansion
set CLASSPATH=.
for %%f in (lib\*.jar) do (
    set CLASSPATH=!CLASSPATH!;%%f
)
echo CLASSPATH: !CLASSPATH!
set CLASSPATH=!CLASSPATH!
REM java -Dfile.encoding=UTF-8 -cp !CLASSPATH! -Xmx1536m groovy.ui.GroovyMain PostgresMigrator.groovy
java -Dfile.encoding=UTF-8 -cp !CLASSPATH! -Xmx1536m groovy.ui.GroovyMain UiMain.groovy
