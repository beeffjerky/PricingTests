import win32com.client as win32
import sys
import subprocess
import shutil
import time
import threading
import os
import datetime
import fnmatch
import traceback
import re
import Queue

HELP = """NAME
    program_test.py

SYNOPSIS
    program_test.py [OPTION] Values PricingA.exe PricingB.exe
    
    Values is a list of programs to be tested if multiple place inside quotes "prog45321\ValidusFQ prog32412 prog34513\AIR" 
    PricingA.exe and PricingB.exe are the absolute path of the two pricing executables to be compared 

OPTION
    -h 
        Print this message and exits
        
    -t  <Threading>
        Runs PricingA single threaded and with only Pricing Instruction (for Feng's Parallel Branch)
        
    -cwd 
        Runs programs inside the current working directory \\Runs and output at \\Results
        Otherwise the run will be done to \\\\CAVCAN03\\work\\Test\\pricingtest
        
    -p  <Output Directory>
        Specifies the location of the output different to the run location 
        
    -r <Root>
        Specifies location of run_pricing.py and pricecompare.py default "\\\\CAVCAN03\\work\\mike_zhou"
        
    -l 
        Threading limit, maximum number of running pricings at once. Default 6
        
    -c 
        Compares only, no runs done
        
    -z 
        Turns off payoff comparisons
        
    -d [Difference tolerence]
        Provide a relative error factor in decimal form
        
        
DESCRPTION
    This is a price comparison and result compilation script for the VCAPS pricing engine.
    
    Input file has format:
        V:\analysis\clint<client number>\prog<program number>\<Model>
        e.g. E:\work\t99.txt
        contents:
            V:\analysis\clnt6037\prog52292\ValidusFQ
            V:\analysis\clnt6042\prog46491\Validus
            V:\analysis\clnt6042\prog46491\ValidusFQ
            V:\analysis\clnt6046\prog44549\Validus
            V:\analysis\clnt6046\prog44549\ValidusFQ
            V:\analysis\clnt6046\prog44550\Validus
            V:\analysis\clnt6046\prog44550\ValidusFQ
            V:\analysis\clnt6047\prog46403\Validus
            V:\analysis\clnt6047\prog46403\ValidusFQ
        I.E. a newline delimited list of program folders
        
        
"""
limit = 6
allThreads = []
list = Queue.Queue()
testDir = "\\\\CAVCAN03\\work\\Test\\pricingtest"
custom = False
parallelTest = ""
zscore_switch = ""
difference_tolerence = ""
inputFile = ""
resultDir = ""
scrapeDir = "\\\\CAVCWEB03\\VCAPS\\analysis\\"

compare = 0
root = "\\\\CAVCAN03\\work\\mike_zhou\\pricingtestswc"

def Start():
    element ="program_test_most_recent"
    try:
        options = parallelTest+ ' -r '+testDir +"\\Runs"+' -l '+str(limit)+' -p '+resultDir+'\\Results\\'+element + ' ' + os.getcwd()+"\\program_test_most_recent.txt"+" "

        executables = pricingA+" "+pricingB
        print ("-------------------------------------~~~"+element+"~~~-------------------------------------")
        if not compare:
            subprocess.call('python '+root+'\\run_pricing.py  ' + options + executables)
        options = zscore_switch+" "+difference_tolerence+' -f '+os.getcwd()+"\\program_test_most_recent.txt"+' -r '+testDir +"\\Runs -t "+resultDir+"\\Results\\"+element
        subprocess.call('python '+root+'\\pricecompare.py ' + options)
        print ("~~~"+element+"-end~~~")
    except Exception:
        traceback.print_exc()
        list.task_done()
        
def Create_instructions():
    programs={}
    print "Create_instructions"
    for file in os.listdir(scrapeDir):
        match =re.findall('\.(\w+)',file)
        if not (match):
            for programme in os.listdir(scrapeDir+file):
                prog_match = re.findall('prog(\d+)',programme)
                if (prog_match):
                    programs["prog"+prog_match[0]] = file
    print "writing to file "+os.getcwd()+"\\program_test_most_recent.txt"
    with open (os.getcwd()+"\\program_test_most_recent.txt", "w+") as infile:
        types = ["\\AIR", "\\RMS","\\RMSFQ",  "\\Validus","\\ValidusFQ"]

        for value in inputFile:
            temp = value.split("\\")
            if len(temp) == 1 : 
                try:
                    for type in types:
                        if os.path.exists(scrapeDir + programs[temp[0]]+"\\"+temp[0] + type):
                            infile.write(scrapeDir + programs[temp[0]]+"\\"+temp[0]+ type+"\n")
                except KeyError:
                    traceback.print_exc()
                    sys.stderr.write("Program "+temp[0]+" couldn't be found on the server")
                    quit()
            elif len(temp) == 2:
                try:
                    if os.path.exists(scrapeDir+programs[temp[0]]+"\\"+temp[0]+"\\"+temp[1]):
                        infile.write(scrapeDir+programs[temp[0]]+"\\"+temp[0]+"\\"+temp[1]+"\n")
                except KeyError: 
                    traceback.print_exc()
                    sys.stderr.write("Program "+temp[0]+" couldn't be found on the server")
                    quit()
    
def ParseArgs():
    global pricingA
    global pricingB
    global inputFile
    global parallelTest
    global limit
    global resultDir
    global testDir
    global custom
    global root
    global compare
    global zscore_switch
    global difference_tolerence
    global scrapeDir
    offset = 0
    args = sys.argv
    
    try:
        if len(args) < 2:
            sys.stderr.write(HELP)
            quit()
        for i in range(0, len(args)):
            if args[i] == "-t":
                parallelTest = "-t "
            elif args[i] == "-p":
                i += 1
                testDir = args[i]
            elif args[i] == "-l":
                i += 1
                limit = args[i]
            elif args[i] == "-h":
                print (HELP)
                quit()
            elif args[i] == "-cwd":
                testDir = os.getcwd()
            elif args[i] == "-r":
                i +=1
                root = args[i]
            elif args[i] == "-c":
                compare = 1
            elif args[i] == "-z":
                zscore_switch = "-z"
            elif args[i] == "-d":
                args +=1
                difference_tolerence = "-d "+args[i]
        if not len(args):
            raise ValueError("No arguments given")
            
        if not resultDir:
            resultDir = testDir
        inputFile = args[-3].split(" ")
        pricingA =  args[-2]
        pricingB =  args[-1]
        

    except ValueError: 
        traceback.print_exc()
        sys.stderr.write(HELP)
        quit()
    
def Main():
    ParseArgs()
    Create_instructions()
    Start()

    
#In case there is ever a need to use this.
#call this function to send an email with the full paths for attatchments
# def Emailer (recipient, subject, body, attatchment = None):
    # if attatchment is None:
        # attatchment = []
        
    # outlook = win32.Dispatch('outlook.application')
    # mail = outlook.CreateItem(0)
    # mail.To = recipient
    # mail.Subject = subject
    # mail.body = body
    
    # for item in attatchment:
        # mail.Attachments.Add(item)
    # mail.send
#Here is an example 
#Emailer ('mike.zhou@validusresearch.com', 'water water water','loo loo loo', ["\\\\CAVCAN03\\work\\Alex\\PricingTest\\pricingtest\\bin\\bigResult\\t32\\Difference_Summary.txt","\\\\CAVCAN03\\work\\Alex\\PricingTest\\pricingtest\\bin\\bigResult\\t32\\Performance_Summary.txt"])


Main()
