import json


srcfile="rawMOT1.csv"
destfile="rawMOT.json"

with open(srcfile) as f:
	srcContent = f.readlines()

result =  []

srcCount = len(srcContent)

for line in srcContent[1:]:
    item = {}
    values  = line.split(",")
    item["REG"] = values[0]
    item["DATE"] = values[1]
    item["NAME"] =  values[5]
    item["PHONE NUMBER"] =  values[6][:-1]
    
    
    key =  values[0]+"-"+ values[1]
    
    
    car = {"MAKE": values[2], "MODEL": values[3],"COLOUR": values[4]}
    item["CAR"] = car
    
    #item["PERSON"] = {"NAME": values[5], "PHONE NUMBER": values[6][:-1]}
    
    result.append(item)
 
destCount = len(result)

print(f"Before {srcfile} {srcCount} lines")
print(f"After {destfile} {destCount} lines")
with open(destfile, "w") as f:
    
    jsonStr = json.dumps(result,indent=4)
    f.write(jsonStr)
   
    #item["car"].append(
    