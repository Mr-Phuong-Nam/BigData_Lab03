import os
inputPath = "taxi-data/"
list_files = os.listdir(inputPath)
sorted_list_files = sorted(list_files)
list_files = [inputPath + file for file in sorted_list_files]
for file in list_files:
    print(file)