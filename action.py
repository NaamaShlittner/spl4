from persistence import *

import sys

def main(args : list[str]):
    inputfilename : str = args[1]
    with open(inputfilename) as inputfile:
        for line in inputfile:
            splittedline : list[str] = line.strip().split(", ")
            act = Activitie(int(splittedline[0]), int(splittedline[1]), int(splittedline[2]), splittedline[3])
            if repo.check_activity(act):
                repo.add_activity(act)

if __name__ == '__main__':
    main(sys.argv)