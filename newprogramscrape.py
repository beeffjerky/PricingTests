import sys
import subprocess
import shutil
import time
import threading
import os
import datetime
import fnmatch
import traceback
import Queue
import os.path
import re
from time import gmtime
from random import randint

HELP = """NAME
    newprogramscrape.py

SYNOPSIS
    newprogramscrape.py [OPTION] Days Programs

OPTION
    -f <Abs path to output file>
        Outputs to the file. Otherwise just outputs to cwd+"\\test_instructions.txt"
        
    -d 
        Dumps the supposed contents of the file to the command line

DESCRIPTION
    Creates a instruction txt file for other tests by scraping the 
    upper n Days of clients for p number of Programs
"""


output_file = ""
scrapeDir = "\\\\CAVCWEB03\\vcaps\\analysis\\"
dump = 0
def Newdatetime(file):
    return os.path.getmtime(scrapeDir+file)
    
def Instructiondatetime(folder):
    return os.path.getmtime(scrapeDir+folder + "\\instruction-pricing.txt")
    
def Reservoir(days,size,list):
    reservoir = []
    reservoir = list[:size]
    counter = size
    
    while (counter < len(list)):
        randomint = randint(0,counter)
        if randomint < size :
            reservoir[randomint] = list[counter]
            counter+=1
        else :
            counter+=1
    return reservoir

def PrintInstruction(reservoir,days, size):
    programs = []
    counter = 0
    types = ["\\AIR", "\\RMS","\\RMSFQ",  "\\Validus","\\ValidusFQ"]
    if not dump:        
        print "Finding pricing instructions"
    else:
        print "Dumping to stdout"
    while counter > 
    for file in reservoir:
        for type in types:
            if os.path.exists(scrapeDir + file + type+ "\\instruction-pricing.txt"):
                if ((abs(current_date-Newdatetime(file + type+ "\\instruction-pricing.txt")))<= days*60*60*24 ):
                    if dump:
                        print scrapeDir + file + type 
                    else :
                        programs.append(file + type)
    
    print "Sorting instructions"
    programs = sorted(programs, key = Instructiondatetime,reverse = True )
    if len(programs) > int(inputFile[2]):
        programs= Reservoir(int (inputFile[1]),int(inputFile[2]), programs)
    if not dump:
        print "Printing to "+output_file
        infile = open (output_file, "w+")
        for program in programs:
            infile.write(scrapeDir+program+"\n")
        infile.close()
    
def ParseArgs():
    global dump
    global output_file
    global inputFile
    args = sys.argv
    
    for i in range(len(args)):
        if args[i] == "-d":
            dump = 1
        elif args[i] == "-f":
            i += 1
            output_file = args[i]
    inputFile = args[-3:]
    if not output_file:
        output_file = os.getcwd()+"\\test_instructions.txt"
    
  
    
def Main():
    
    global current_date
    global output_file
    ParseArgs()
    if inputFile[-1] == "-d":
        dump = 1
    folderlist=[]
    for file in os.listdir(scrapeDir):
        match =re.findall('\.(\w+)',file)
        if not (match):
            folderlist.append(file)
    folderlist = sorted(folderlist, key = Newdatetime,reverse = True )
    
    
    current_date = Newdatetime(folderlist[0])
    print "Compiled Client List:\t"+ str(len(folderlist)) + "\tTestable Clients found"
    
    reservoir= Reservoir(int(inputFile[1])*100,len(folderlist), folderlist)
    folderlist = []
    for client in reservoir:
        for file in os.listdir(scrapeDir+client):
            match =re.findall('\.(\w+)',file)
            if not (match):
                folderlist.append(client+"\\"+file)
    
   

    reservoir= Reservoir(int (inputFile[1])*100,len(folderlist), folderlist)
    print "Compiled Program list:\t"+ str(len(folderlist)) + "\tTestable Programs found"
    folderlist = sorted(folderlist, key = Newdatetime,reverse = False )
    PrintInstruction(reservoir,int (inputFile[1]),int(inputFile[2]))

Main()