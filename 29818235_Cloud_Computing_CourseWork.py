#!/usr/bin/env python
# coding: utf-8

# In[3]:


#Name: Alexis Courtois-Gritsanchouk                   
#ID: 29818235

# used to optimise code with a script per pipeline stage and parameters in a config ".yaml" file
import yaml
#required to be used for .yaml file for optimising code with a script per pipeline stage and parameters
import os
#used for sleep function
import time
#used for timings
import timeit
#used for multithreading
import threading
#used for exiting program
import sys
#to manipulate data as a dataframe
import pandas as pd
#to preprosess data
from sklearn import preprocessing
#to visualise data and results
import matplotlib.pyplot as plt
#for visualisations
import seaborn as sns #Visualization
#to convert from integers to strings
from sklearn.preprocessing import LabelEncoder
#classification reports confusion matrixs etc.
from sklearn.metrics import classification_report, confusion_matrix, precision_recall_curve, auc, roc_curve
import numpy as np

#import dataset
dataPassengers = pd.read_csv('AComp_Passenger_data_no_error(1).csv')
dataAirport = pd.read_csv('Top30_airports_LatLong(1).csv')


# folder to load config file
CONFIG_PATH = "C:/Users/44739" 
#CONFIG_PATH = "alexy244/29818235_Cloud_Computing_CourseWork"

# Function to load yaml configuration file
def load_config(config_name):
    with open(os.path.join(CONFIG_PATH, config_name)) as file:
        config = yaml.safe_load(file)

    return config

config = load_config("Pipeline2.yaml") #fetching pipeline.yaml file


print("Displaying Passengers Data:")
display(dataPassengers)
print("\nDisplaying Airports Data:")
display(dataAirport)

list1 = dataPassengers["From Airport Acronym"]
#list1.pop(config["Pop_Index"])               #used to check an see if the error handling section of code works correctly 
#list1[config["NoN_Value_Index"]] = config["Non_Value"]           #used to check an see if the error handling section of code works correctly
#list1[config["NoN_Value_Index"]] = config["Non_Value2"]          #used to check an see if the error handling section of code works correctly 
print("\n Airport Acronym Column:\n", list1)

list2 = dataPassengers["Passenger ID"]
print("\n Passengers ID Column:\n", list2)

data_pairs = []

file_object = open(config["File_Name"],"w+") #creating seperate file to save output to (like in hadoop)



def mapper(value):                                                   #for mapping
    for item in value:                                               #for each item in the list
        if len(item) == 0 or item == "NaN" or item == "NAN" or item == "Nan" or item == "nan" or item == "naN": #checking to see if all values have data
            print("\n" + config["error_message"] + "\n")
            time.sleep(config["sleeping_time"])
            sys.exit()
        else:
            temp = item +' ' + str(1)                                #take each item and add one on the end
            data_pairs.append(temp)                                  #append (add) each item together
    data_pairs.sort()                                                #sort in alphabetical order
    print("\n All Individual Values Found In List: \n", data_pairs)  #print list of values
   

def reducer(value):                                                                      #for reducing
    data_pairs_final = list(dict.fromkeys(data_pairs))                                   #create only unique value list
    print("\n Final Unique Values List: \n", data_pairs_final)                           #printing unique values
    print("\n Final Unique Values List Length: \n", len(data_pairs_final))               #printing length of unique values
    print("\n Final Reduced List Of All Unique Values And The Number Of Each One: \n")
    for item in data_pairs_final:                                                        #for each item in list
        count = data_pairs.count(item)                                                   #count how many are the same
        Final_List=item.split(' ')[0] + ":" + str(count)                             #keep only one of each value (unique) and add the number of counts of each unique value
        file_object.write("\n")
        file_object.write(Final_List)                                                #saving output to external .txt file also
        print(Final_List)                                                            #printing final list of all unique values



if __name__ == '__main__':
    start = timeit.default_timer()
    t1 = threading.Thread(target=mapper(list1)) #creating thread
    end = timeit.default_timer()
    Map1 = (end - start)
    print("\nTime It Took For Mapper Function To Calculate In Seconds: ", Map1)

    start2 = timeit.default_timer()
    t2 = threading.Thread(target=reducer(list1)) #creating thread
    end2 = timeit.default_timer()
    Reduce1 = end2 - start2
    print("Time It Took For Reducer Function To Calculate In Seconds: ", Reduce1)
    
    #starting threads
    t1.start()
    t2.start()
    
    #joining threads
    t1.join() 
    t2.join() 
    
    print("\n")
    data_pairs = []
    file_object.write("\n")
    
    start3 = timeit.default_timer()
    t3 = threading.Thread(target=mapper(list2)) #creating thread
    end3 = timeit.default_timer()
    Map2 = end3 - start3
    print("\nTime It Took For Mapper Function To Calculate In Seconds: ", Map2)

    start4 = timeit.default_timer()
    t4 = threading.Thread(target=reducer(list2)) #creating thread
    end4 = timeit.default_timer()
    Reduce2 = end4 - start4
    print("\nTime It Took For Reducer Function To Calculate In Seconds: ", Reduce2)
    
    #starting threads
    t3.start()
    t4.start()
    
    #joining threads
    t3.join() 
    t4.join() 

    
    ################ using seaborn unique function to cross reference with own created map reduce program to check results ################ 
    print("\n using seaborn unique to cross reference with own created map reduce program: \n")
    start5 = timeit.default_timer()
    print(dataPassengers['From Airport Acronym'].unique()) #what unique values in From Airport Acronym column
    end5 = timeit.default_timer()
    MapSeaborn1 = end5 - start5
    print("Time It Took For Unique Function Mapping from seaborn To Calculate In Seconds: ", MapSeaborn1)
    print("")
    start6 = timeit.default_timer()
    print(dataPassengers.groupby('From Airport Acronym').size()) #number of unique values in From Airport Acronym column 
    end6 = timeit.default_timer()
    ReduceSeaborn1 = end6 - start6
    print("Time It Took For Reducer from seaborn To Calculate In Seconds: ", ReduceSeaborn1)
    
    f2 = plt.figure(figsize=(config["Fig_Size3"]))
    sns.countplot(dataPassengers['From Airport Acronym'],label="Count") #displaying bar chart with counts of each unique value

    print("\n")
    start7 = timeit.default_timer()
    print(dataPassengers['Passenger ID'].unique()) #what unique values in From Airport Acronym column
    end7 = timeit.default_timer()
    MapSeaborn2 = end7 - start7
    print("Time It Took For Unique Mapping Function from seaborn To Calculate In Seconds: ", MapSeaborn2)
    print("")
    start8 = timeit.default_timer()
    print(dataPassengers.groupby('Passenger ID').size()) #number of unique values in From Airport Acronym column 
    end8 = timeit.default_timer()
    ReduceSeaborn2 = end8 - start8
    print("Time It Took For Reducer from seaborn To Calculate In Seconds: ", ReduceSeaborn2)
                                                                                                            
    f3 = plt.figure(figsize=(config["Fig_Size2"]))
    sns.countplot(dataPassengers['Passenger ID'],label="Count") #displaying bar chart with counts of each unique value
    file_object.close()
    
    
    #bar chart with execution times
    f4 = plt.figure(figsize=(config["Fig_Size2"]))
    N = config["N"]
    All_Mapper = (config["Average_Mapper_Times"])
    All_Reducer = (config["Average_Reducer_Times"])

    ind = np.arange(N) 
    width = config["bar_chart_widths"]     
    plt.bar(ind, All_Mapper, width, label='Mapping Time')
    plt.bar(ind + width, All_Reducer, width,
    label='Reducing Time')

    plt.ylabel('Time To Execute  (Seconds)')
    plt.title('Averages Of Time Taken To Execute MapReduce Functions')

    plt.xticks(ind + width / 2, ('My MapReduce Function Task 1', 'My MapReduce Function Task 2', 'Seaborn Unique MapReduce Function Task 1', 'Seaborn Unique MapReduce Function Task 2'))
    plt.legend(loc='best')
    plt.show()
    
    #second bar chart with execution times that uses live values, can be a bit buggy as some time the timers show 0 seconds for no reason
    f5 = plt.figure(figsize=(config["Fig_Size"]))
    N = config["N"]
    All_Mapper = (Map1, Map2, MapSeaborn1, MapSeaborn2)
    All_Reducer = (Reduce1, Reduce2, ReduceSeaborn1, ReduceSeaborn2)

    ind = np.arange(N) 
    width = config["bar_chart_widths"]      
    plt.bar(ind, All_Mapper, width, label='Mapping Time')
    plt.bar(ind + width, All_Reducer, width,
    label='Reducing Time')

    plt.ylabel('Time To Execute  (Seconds)')
    plt.title('Current Time Taken To Execute MapReduce Functions')

    plt.xticks(ind + width / 2, ('My MapReduce Function Task 1', 'My MapReduce Function Task 2', 'Seaborn Unique MapReduce Function Task 1', 'Seaborn Unique MapReduce Function Task 2'))
    plt.legend(loc='best')
    plt.show()


# In[ ]:




