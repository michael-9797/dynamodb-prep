import json


#filename=r"C:\Users\Terry's\Documents\Database\DB-IMPORT\rawMOT.json"
filename = "rawMOT.json"


def findEmptyStrings(jsonDict,keys=["REG", "DATE"]):
    for key in keys:
        if jsonDict[key] == "":
            #print(jsonDict)
            return True
    return False


with open(filename) as f:
    raw = f.read()
    
parsed = json.loads(raw)

linenumber = 2
err_count = 0
none_empty = []
cleaned_ls = []
key_count = {}
empty_count=0
for jsonDict in parsed:
    linenumber += 13
    res = findEmptyStrings(jsonDict)
    if res:
        empty_count +=1
        continue
    none_empty.append(jsonDict)
    primary_key = f"{jsonDict['REG']}_{jsonDict['DATE']}"
    if not primary_key in key_count:
        key_count[primary_key] = {"item": jsonDict , "count": 1}
    else:
        key_count[primary_key]["item"] = jsonDict
        key_count[primary_key]["count"] += 1
        
dupe_count =0
for  key in key_count:
    if key_count[key]["count"] > 1:
        dupe_count += ( key_count[key]["count"] -1 )
         
        #print(f"{key}")
        pass
    #elif key_count[key]["count"] == 1: 
    else:
        cleaned_ls.append(key_count[key]["item"])


with open("FinalMOT.json", "w") as f:
    dict_str = json.dumps(cleaned_ls, indent=4)
    f.write(dict_str)


#with open("cleanedMOTv2.json", "w") as f:
#    dict_str = json.dumps(none_empty, indent=4)
#    f.write(dict_str)

print(f"Total items before: {len(parsed)}")
print(f"Empty count: {empty_count}")
print(f"Dupe count: {dupe_count}")
print(f"Total items after - FinalMOT.json: {len(cleaned_ls)}")
#print(f"Total items - cleanedMOTv2.json: {len(none_empty)}")


