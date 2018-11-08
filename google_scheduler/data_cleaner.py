import sys

def get_only_useful_rows():
    file = "/mnt/google_data/google/task_events/part-00{:03d}-of-00500.csv"
    new_file = "google_events_data.csv"
    g = open(new_file,'w')
    for i in range(500):
        file_path = file.format(i)
        print file_path
        f = open(file_path,'r')
        for line in f:
            lst = line.split(",")
            if not lst[1]:
                if int(lst[5]) in [1,2,3,4,5,6,8]:
                    g.write(",".join([lst[3],lst[2],lst[0],lst[9],lst[10],lst[5]]))
                    g.write('\n')
        f.close()
        sys.stdout.flush()
    g.close()
    
get_only_useful_rows()