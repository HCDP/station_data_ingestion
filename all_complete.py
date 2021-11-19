from sys import argv
import json

def all_complete():
    for i in range(1, len(argv)):
        file = argv[i]
        state_data = None
        with open(file, "r") as f:
            state_data = json.load(f)
        if not state_data["complete"]:
            return False
    return True

if all_complete():
    print(1, end = "")
else:
    print(0, end = "")