# goRareLogDetector  
This tool finds log records rarely appeared.  
Useful when trouble shooting using log files.  
This tool is also intended to find abnormal logs.  
  
## Prerequisits  
This application will run on Linux.  
Tested on Ubuntu10.04, Ubuntu22.04, CentOS6, CentOS7, Debian8, Debian11.  
There are no dependencies.  
  
## Installation  
The binary file "rarelog" in this repository is an executable file.  
You can just use it.  
This can be re-compiled by 
```
# scripts/install.sh
```  
  
## Usage  
Search the top 10 rare log records.  
```
# ./rarelog '/var/log/syslog*'
```  
  
But it will take time and need RAM depending on the log size.  
Instead you can use storage cache by -d option.  
```
# ./rarelog '/var/log/syslog*' -d logcache
```  
  
And next time you can run the way below  
```
# ./rarelog -d logcache
```  
The log position is saved inside logcache, so the tool will start reading the log from the last position next time.  
  
### Running modes (-m option)
- topN  
Shows top 10 rare log records.  
This is the default mode.  
You can use N, M, D option to change the behaviour.  
```
# ./rarelog -N 30 -M 3 -D 3
```  
for details run  
```
# ./rarelog -help
```  
  
- detect  
Shows the count of similar log records for each new log record.  
```
<count>,<log record>
```  
Command line example  
```
# ./rarelog -m detect -d logcache
```  
It is recommended to execute this mode after executing with "feed" mode, because the log records can be huge.   
  
  
- feed  
Analyze the log files and only saves to the cache.  
Command line example  
```
# ./rarelog -m feed -f '/var/log/syslog*' -d logcache
```  
  
### More detailed analyzation  
You can parse logs more efficiently by specifying the log formant and timestamp format.  
You can do this by preparing a yaml file with the format below.  
```
dataDir: "{{ HOME }}/rarelogs/Test_main_config/data" # directory of cache
logPath: "../../test/data/rarelogdetector/analyzer/sample.log*" # log path regex
searchString: ERROR # regex to include
excludeString: always # regex to exclude
logFormat: '^(?P<timestamp>\w+ \d+ \d+:\d+:\d+) \[\d+\]\[\w+\] (?P<message>.+)$'
timestampLayout: "Jan 2 15:04:05"
daysToKeep: 7 # Days to keep log cache.
```  
Command line example  
```
# ./rarelog -c <config path>
```  
  
## More options  
There are more options.  
Check by 
```
# ./rarelog -help
```  

