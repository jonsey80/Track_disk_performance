This is a service agent job which currently set to run once a week- it runs a sproc which will gather the following details:

Latency
disk stalls 
which Azure Gen 1 IOPS bucket the disk will be in 

This helps asses if more space/iops needs to be assigned to the instance or not 

to use add in a database name you wish to install the scripts to and alter the sa login to the renamed version off the instance 
