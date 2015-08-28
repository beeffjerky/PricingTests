import sys
import traceback
import os
import Queue
import threading
import subprocess
import time
import datetime

import shutil
executables = {'A':"Q:\\bin\\pricing_20150706.exe","B":"V:\\bin\\pricing.exe"}
instructionfile = "E:\\work\\Alex\\PricingTest\\pricingtest\\bin\\t200.txt"
root = "\\\\CAVCAN03\\work\\Test\\pricingtest\\Runs"
result = "\\\\CAVCAN03\\work\\Test\\pricingtest\\Result\\Most_Recent"
source = "\\\\CAVCWEB03\\vcaps\\analysis\\"
pricing_A = "\\\\CAVCWEB03\\vcaps\\bin\\pricing.exe"
pricing_B = "\\\\CAVCWEB03\\vcaps\\bin\\pricing.exe"
printLock = threading.Lock()
performanceLock = threading.Lock()
programs = Queue.Queue()
programing = [Queue.Queue(),Queue.Queue()]
performance = {}

parallel_test = 0
limit = 6
HELP = """NAME
    run_pricing.py

SYNOPSIS
    run_pricing.py [OPTION] InputFile PricingA.exe PricingB.exe

OPTION
    -h 
        Print this message and exits
    -t 
        Runs PricingA single threaded and with only Pricing Instruction (for Feng's Parallel Branch)
        
    -r  <Running Folder>
        Specifies the folder that the runs are stored in. Default \\\\CAVCAN03\\work\\Test\\pricingtest\\Runs
        
    -s  <Source Directory>
        Specifies the folder containing the programs to be tested Default "\\\\CAVCWEB03\\vcaps\\analysis\\"
        
    -p  <Output Directory>
        Specifies the folder for the performance summary output. Default \\\\CAVCAN03\\work\\Test\\pricingtest\\Result\\Most_Recent
        
    -l 
        Threading limit, maximum number of running pricings at once. Default 6
        
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
def SavePerformance(key, value):
    with performanceLock:
        performance[key] = value
        
def GetPerformance(key):
    with performanceLock:
        return performance[key]
        
## read an instruction file to a dictionary
def OpenInstruction(location):
    instFile = open(location, "r")
    ins = {}
    for line in instFile:
        if not(line == "" or line.startswith("//")):
            temp = line.rstrip("\n").split(" = ")
            if(len(temp) > 0):
                ins[temp[0]] = temp[1]
    instFile.close()
    return ins
    
## save a dictionary to as an instruction file
def SaveInstruction(ins, location):
    instFile = open(location, "w")
    for key, value in ins.items():
        instFile.write(key + " = " + value + "\n")
    instFile.close()
    return
    
def ModifyInstructions(locationDir):
    ## read in the instruction file
    insA = OpenInstruction(locationDir + "_run_A\\instruction-pricing.txt")
    insB = OpenInstruction(locationDir + "_run_B\\instruction-pricing.txt")
    
    ## modify the instructions
    modelName = insA["modelName"].rstrip("\n")
    temp = ""
    if modelName.startswith("V"):
        temp = "V";
    elif modelName.startswith("RMS"):
        temp = "RMS";                  
    elif modelName.startswith("AIR"):
        temp = "AIR";                  
    elif modelName.startswith("EQE"):
        temp = "EQE";
        
    modelNum = insA["modelVersion" + temp].rstrip("\n")
    
    insA["pathToLayerPayoffIterEventFiles"] = root + "\\payoffA\\"
    insA["pathToLayerPayoffTerrorEventFiles"] = root + "\\payoffTerrorA\\"
    insB["pathToLayerPayoffIterEventFiles"] = root + "\\payoffB\\"
    insB["pathToLayerPayoffTerrorEventFiles"] = root + "\\payoffTerrorB\\"
    
    ## save the instructionss
    SaveInstruction(insA, locationDir + "_run_A\\instruction-pricing.txt")
    SaveInstruction(insB, locationDir + "_run_B\\instruction-pricing.txt")

    ## creates the payoff directories
    file = root + "\\payoffA\\adjusted\\" + modelName + modelNum
    if not(os.path.exists(file)):
        os.makedirs(file)
    file = root + "\\payoffTerrorA\\adjusted\\" + modelName + modelNum
    if not(os.path.exists(file)):
        os.makedirs(file)
    file = root + "\\payoffB\\adjusted\\" + modelName + modelNum
    if not(os.path.exists(file)):
        os.makedirs(file)
    file = root + "\\payoffTerrorB\\adjusted\\" + modelName + modelNum
    if not(os.path.exists(file)):
        os.makedirs(file)
    
    
    insA = OpenInstruction(locationDir + "_run_A\\instruction-impact.txt")
    insA["pathToLayerPayoffIterEventFiles"] = root + "\\payoffA\\"
    SaveInstruction(insA, locationDir + "_run_A\\instruction-impact.txt")
    
    insB = OpenInstruction(locationDir + "_run_B\\instruction-impact.txt")
    insB["pathToLayerPayoffIterEventFiles"] = root + "\\payoffB\\"
    SaveInstruction(insB, locationDir + "_run_B\\instruction-impact.txt")
    
    return

def ThreadPrint(out):
    with printLock:
        print (out)
    return
    
def Start(ABSelect):
    try:
        while True:
            run_folder = programing[ABSelect].get(False)
            prog = run_folder.split("\\")[-2][4:]
            model = run_folder.split("\\")[-1]
            start = datetime.datetime.now()
            for instFile in ["instruction-pricing","instruction-impact"]:
                if instFile =="instruction-impact" and ABSelect and parallel_test:
                    pass
                elif ABSelect:
                    with open (run_folder+"_run_A\\"+instFile+"-run_output.txt", "w+") as outfile:
                        ThreadPrint(run_folder+"_run_A\\"+instFile)
                        temp = subprocess.call(pricing_A +" "+ instFile+".txt", cwd = run_folder+"_run_A\\", stdout=outfile, stderr=subprocess.STDOUT);
                else:
                    with open (run_folder+"_run_B\\"+instFile+"-run_output.txt", "w+") as outfile:
                        ThreadPrint(run_folder+"_run_B\\"+instFile)
                        temp = subprocess.call(pricing_B +" " +instFile+".txt", cwd = run_folder+"_run_B\\", stdout=outfile, stderr=subprocess.STDOUT);
            end = datetime.datetime.now()    
            if ABSelect:
                SavePerformance(prog+"\t"+model, str(end-start))
            else:
                prog_performance = GetPerformance(prog+"\t"+model)
                SavePerformance(prog+"\t"+model, prog_performance +"\t"+ str(end-start))
    except Queue.Empty:
        pass
    return
    
        
def FolderSetUp(line):
    temp = line.split("\\")
    sourceDir = source + temp[len(temp) - 3] + "\\" + temp[len(temp) - 2] + "\\"+temp[len(temp) - 1]+"\\"
    destDir = root +"\\" + temp[len(temp) - 2] + "\\"+temp[len(temp) - 1]
    ## ensure the source and destination directories exist
    if not (os.path.exists(sourceDir)):
        ThreadPrint("Error cannot find " + sourceDir)
        return
    if not(os.path.exists(destDir + "_run_A")):
        os.makedirs(destDir + "_run_A")
        
    if not(os.path.exists(destDir + "_run_B")):
        os.makedirs(destDir + "_run_B")
    
    ## copy the files necessary for execution
    fileList = ["layers.txt", "instruction-pricing.txt", "probs-for-allocation.txt", "probs.txt", "probs2.txt", "instruction-impact.txt"]
    
    for file in fileList:
        fileSource = sourceDir + file
        if os.path.isfile(fileSource):
            shutil.copyfile(fileSource, destDir + "_run_A\\" + file)
            shutil.copyfile(fileSource, destDir + "_run_B\\" + file)
        else:
            return 0
    return destDir

                
## read the input string for flags and values
## sets/changes global variables  
def ParseArgs():

    global inputFile
    global instructionfile 
    global root 
    global source
    global pricing_A
    global pricing_B
    global parallel_test
    global limit
    global result
    
    args = sys.argv
    try:
        if len(args) < 4:
             raise ValueError("Incorrect number of Arguments")
        for i in range (0,len(args)):
            if args[i] == "-h":
                sys.stderr.write(HELP)
                quit()
            elif args[i] == "-t":
                parallel_test = 1
            elif args[i] == "-r":
                i +=1
                root = args[i]
                
            elif args[i] == "-s":
                i+=1
                source = args[i]
            elif args[i] == "-p":
                i+=1
                result = args[i]
            elif args[i] == "-l":
                i+=1
                limit = int(args[i])

        instructionfile = args[-3]
        pricing_A = args[-2]
        pricing_B = args[-1]
        if not(os.path.exists(instructionfile)):
            raise ValueError(instructionfile+" not Found")
            
        if not(os.path.exists(pricing_A)):
            raise ValueError(pricing_A+" not Found")
        if not(os.path.exists(pricing_B)):
            raise ValueError(pricing_B+" not Found")
            
        if not(os.path.exists(result)):
            os.makedirs(result)
        
        with open(instructionfile, "r") as infile:
            for line in infile.readlines():
                runs = FolderSetUp(line.rstrip("\n"))
                if runs:
                    ModifyInstructions( runs)
                    programing[0].put(runs) 
                    programing[1].put(runs)
                
    except ValueError: 
        traceback.print_exc()
        sys.stderr.write(HELP)
        quit()
    
def Main():
    ParseArgs()
    allThreads = []
    if parallel_test:
        for i in range(1): 
            t = threading.Thread(target = Start, args = (1,))
            t.start()
            allThreads.append(t)
            time.sleep(1)
        for aThread in allThreads:
            aThread.join()
            
    else:
        for i in range(limit): 
            t = threading.Thread(target = Start, args = (1,))
            t.start()
            allThreads.append(t)
            time.sleep(1)
        for aThread in allThreads:
            aThread.join()
        
    for i in range(limit): 
        t = threading.Thread(target = Start, args = (0,))
        t.start()
        allThreads.append(t)
        time.sleep(1)
    for aThread in allThreads:
        aThread.join()

    with open (result+"\\_performance_summary.txt" ,"w+")  as outfile:
        outfile.write("Program\tModel\tA.EXE Price+Imp\tB.EXE Price+Imp\t"+pricing_A+"\t"+pricing_B+"\n")
        for key in performance:
            outfile.write( key + "\t" + performance[key] + "\n")
    
        
Main()
