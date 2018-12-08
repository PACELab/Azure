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
 
![523](https://user-images.githubusercontent.com/31523851/49683422-b003ac00-fa92-11e8-8323-da656060a97d.jpg)

```
Executor:
This is the core manager file.All code execution starts from here.
Executor invokes and interacts with all the peripheral components.

Feeder :
Feeder reads data from the csv file and provides data to the scheduler line by line

Config :
Config configures the feeder,executor and the algorithm tunin parameters

Algorithm:
Algorithm has the core scheduling logic it accepts a input vm and scedules the vm on a server in the data center
Event based delay schemes
1) round_robin_mvg_avg_delay (vm are dealyed when they cross second threshold  i.e  running_mean + stdmultipier * running_standard_deviation at max by a delay time defined in the config)
NOTE : All the algorithms have the same skeletal structure except for some boundary conditions.
Please refer to the inline comments for the working of the algorithms.
2) round_robin_min_delay(same  as above except the second threshold is static)
3) round_robin_delay_efficient( vms are delayed when the total core usage exceeds a threshold)

Time based delay schemes
Different time based delay schemes are also present description for the same is present in the algorithm files
1) round_robin_delay_v1
2) round_robin_delay_v2
3) round_robin_delay_v3
4) round_robin_delay_v4

Data Visualizer:
Accepts a algorithm object and generates graphs for different parameters

Supporting Logs:
Contains several execution stats like
1) Max vm delay time
2) Vm delay percentiles
3) Simulation delay time
4) Number of Vm's delayed
5) Max core usage
```
### Supporting scripts
```
1) csv_file_generator.py (reads task_events files of google trace and consolidates them into single google_events_data.csv which will be used by feeder )
2) total_vm_delay_stat_calculator.py (this script calculates the total number of vms * dealy product for a given algorithm)

```
