This framework provides API's  for analysing Azure and Google traces and simulates the data centre behaviour. It also provides  insights about different scheduling schemes.

### How to run ? 
### Set configuration
```
{
  "servers": {
    "number_of_servers": 13000, # total number of servers in the data centre
    "types": {                 
      "A": {
        "server_number":0, # starting server number
        "number": 0,       # total number of servers
        "max_cores_per_server": 8, # maximum number of cores in a server
        "max_ram_per_server": 64   # maximum ram in a server
      },
      "B":{
        "server_number":0,
        "number": 1000000,
        "max_cores_per_server": 16,
        "max_ram_per_server": 128
      }
    }
   },
  "mode":"execute", # choose from execute and debug modes
  "feeder_file_path": "scheduler/google_events_data.csv", #file path for the feeder which sends data to the scheduler
  "algorithm": "round_robin_mvg_avg_delay",# type of scheduling algorithm used
  "actual_output_path":"scheduler/Graphs/round_robin_mvg_avg_delay/config_16/actual_output.csv",
  "reference_output_path":"scheduler/Graphs/round_robin_mvg_avg_delay/config_16/reference_output.csv",
  "window_size":1,#number of days to calculate mean.
  "stdMultiplier":1,#standard deviation multiplier
  "delay_time":600000000,#maximum delay that a vm could be delayed
  "cores_hard_limit":7700,# maximum number of cores that can be present in the data center
}

```
### Execute the below command in scheduler path
```
  nohup python2.7 executor.py > mvg_avg_5day_10min.txt 2>&1 &
  
```
### Graphs
```
Path = ./Graphs/<algorithm>/config_16
```
### Design
  
```
![alt text](https://www.google.com/search?q=google&rlz=1C1CHBF_enIN729IN729&source=lnms&tbm=isch&sa=X&ved=0ahUKEwjS3PH91Y_fAhUMTt8KHb7QChsQ_AUIESgE&biw=1242&bih=604#imgrc=np0mvtYJ3iaxDM:)
           config
             |
             v
Feeder -> executor -> Algorithm -> Graphs
                                \_> 
This framework consists of these components
1)Feeder (a basic generator which reads data from a given csv line by line)
2)
3)
```
