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

HELP = """NAME
    PricingTest.py

SYNOPSIS
    PricingTest.py [OPTION] InputFile PricingExePathA PricingExePathB

OPTION
    -t
        Run impact results
        
    -l [NUM]
        The maximum number of threads to run. Default is 5

    -e [0|1|2|3]
        The number of executables to run
        0 comparison only (default)
        1 only run A
        2 only run B
        3 run A and B
        Note: If an output file cannot be found,
        the corresponding executable will be (re)run
    
    -r [PATH]
        Specify a location where the results will be stored.
        If the folder does not exist it will be created
        If no folder is specified the current directory will be used
        This will cause a Difference_Summary file to be generated
    
    -h 
        Print this message and exits
"""

## if limit is changed remember to update info in HELP
limit = 5
lines = Queue.Queue()

performanceResults = [["PROG", "Model", "Pricing/Impact", "CPU Runtime A", "CPU Runtime B"]]

root = "\\\\CAVCAN03\\work\\Edison\\pricingtest\\"
source = "\\\\CAVCWEB03\\vcaps\\analysis\\"
testImpact = 0
numToRun = 0
resultDir = None
inputFile = None
pricingA = ""
pricingB = ""

printLock = threading.Lock()
def ThreadPrint(out):
    with printLock:
        print (out)
    return
 
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

def csvCompare(programID, model):
    payoffList = []
   
    ## A and B should have the same layers file
    runDir = root + "prog" + programID + "\\" + model
    f = open(runDir + '_run_A\\layers.txt', 'r')
    f.readline() # Skip header
    for row in f:
        payoffID = row.split()[0]
        payoffList.append(payoffID)
    f.close()

    modelname = ''
    modelnum = ''
    ## A and B should have the same model information
    f = open(runDir + '_run_A\\instruction-pricing.txt', 'r')
    if model.startswith('V'):
        modelname = 'V'
    if model.startswith('RMS'):
        modelname = 'RMS'
    if model.startswith('AIR'):
        modelname = 'AIR'
    if model.startswith('EQE'):
        modelname = 'EQE'
    for row in f:
        if row.startswith('modelVersion' + modelname):
            modelnum = row.split()[2]
    f.close()

    diff = []
    modelInfo = modelname + modelnum
    for item in payoffList:
        diffFile = "diff_prog" + programID + "_" + model + ".txt"
        if not (resultDir == None):
            diffFile = resultDir + diffFile
        payoffPath = "\\adjusted\\" + modelInfo + "\\iter-event-for-payoff-contract-" + item + ".txt"
        args = "-F csvCompare -f " + root + "payoffA" + payoffPath + " -A " + root + "payoffB" + payoffPath
        args += " -k0-2 -s 1 -c3-6 -b 1 1"
        outFile = open(diffFile, "w")
        errFile = open(os.devnull, "w")
        subprocess.call("zscore.exe " + args, stdout=outFile, stderr=errFile)
        outFile.close()
        errFile.close()
        #ThreadPrint  "\n" + args + "\n"
        
        if not os.stat(diffFile).st_size == 0:
            f = open(diffFile, 'r')
            i = 0
            for line in f:
                i += 1
            temp = []
            temp.append(item)
            temp.append(i)
            diff.append(temp)
    
    if len(diff) != 0:
        outFile = open(diffFile, "w")
        for row in diff:
            i = 0
            for item in row:
                if i == 0:
                    outFile.write(str(item) + "\t")
                else:
                    outFile.write(str(item) + "\n")
                i += 1
        outFile.close()
    #ThreadPrint (diff)
    return

## Starts a PricingCompare.exe
## tries to rerun some pricing executables if their output files are not found
def RunXMLCompare(programID, model):
    path = root + "prog" + programID + "\\" + model
    
    args = " "
    if testImpact:
        args = "-t "
    if not (resultDir == None):
        args += "-r " + resultDir + " "
    
    ## using the printLock here since the program has output I don't want to suppress
    ## the program doesn't take too long to run so it shouldn't cause a bottleneck
    code = 0
    if numToRun > 0:
        with(printLock):
            code = subprocess.call("PricingCompare.exe " + args + path)
    else:
         code = subprocess.call("PricingCompare.exe " + args + path)
         
    ## try to rerun executables if output files were not found
    if (code == 1 or code == 3):
        ThreadPrint ("Rerunning pricing.exe A for Program " + programID + " Model " + model + "\n")
        RunPricing(path + "_run_A\\", 0)
        if testImpact:
           RunPricing(path + "_run_A\\", 1)
    if (code == 2 or code == 3):
        ThreadPrint ("Rerunning pricing.exe B for Program " + programID + " Model " + model + "\n")
        RunPricing(path + "_run_B\\", 0)
        if testImpact:
            RunPricing(path + "_run_B\\", 1)
            
    ## try to rerun the comparison
    if not (code == 0):
        code = subprocess.call("PricingCompare.exe " + args + path)
        
    if not (code == 0):
        ThreadPrint ("Failed to compare the output  for Program " + programID + " Model " + model)
        ThreadPrint ("Please ensure the output files are located in the root\n")
    return 
 
## modifies an instruction file to point to the root
## also creates the payoff directories in the root if they are not present
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
    modelNum = insA["modelVersion" + temp].rstrip("\n");
    
    insA["pathToLayerPayoffIterEventFiles"] = root + "payoffA\\"
    insA["pathToLayerPayoffTerrorEventFiles"] = root + "payoffTerrorA\\"
    insB["pathToLayerPayoffIterEventFiles"] = root + "payoffB\\"
    insB["pathToLayerPayoffTerrorEventFiles"] = root + "payoffTerrorB\\"
    
    ## save the instructions
    SaveInstruction(insA, locationDir + "_run_A\\instruction-pricing.txt")
    SaveInstruction(insB, locationDir + "_run_B\\instruction-pricing.txt")

    ## creates the payoff directories
    file = root + "payoffA\\adjusted\\" + modelName + modelNum
    if not(os.path.exists(file)):
        os.mkdir(file)
    file = root + "payoffTerrorA\\adjusted\\" + modelName + modelNum
    if not(os.path.exists(file)):
        os.mkdirs(file)
    file = root + "payoffB\\adjusted\\" + modelName + modelNum
    if not(os.path.exists(file)):
        os.mkdirs(file)
    file = root + "payoffTerrorB\\adjusted\\" + modelName + modelNum
    if not(os.path.exists(file)):
        os.mkdirs(file)
    
    if testImpact:
        insA = OpenInstruction(locationDir + "_run_A\\instruction-impact.txt")
        insB = OpenInstruction(locationDir + "_run_A\\instruction-impact.txt")
        insA["pathToLayerPayoffIterEventFiles"] = root + "payoffA\\"
        insB["pathToLayerPayoffIterEventFiles"] = root + "payoffB\\"
        SaveInstruction(insA, locationDir + "_run_A\\instruction-impact.txt")
        SaveInstruction(insB, locationDir + "_run_B\\instruction-impact.txt")
    
    return
    
## starts a pricing.exe and suppresses its output
## 'rundir' is the location of the executable and instruction file
## 'impact' is whether to use the impact instructions
## returns the amount of time it took the pricing.exe to run
def RunPricing(runDir, impact):
    instFile = None
    if not(impact):
        instFile = "instruction-pricing.txt"
    else:
        instFile = "instruction-impact.txt"
        
    ## used to suppress the output of the process
    FNULL = open(os.devnull, 'w')

    start = datetime.datetime.now()
    try:
        temp = subprocess.call(runDir + "pricing.exe " + instFile, cwd = runDir, stdout=FNULL, stderr=subprocess.STDOUT);
    except WindowsError:
        ## usually [Error 32]
        sys.stderr.write("Error trying to run " + runDir + "pricing.exe\nRetrying in 1 second\n")
        time.sleep(1)
        ## try to rerun the executable
        start = datetime.datetime.now()
        temp = subprocess.call(runDir + "pricing.exe " + instFile, cwd = runDir, stdout=FNULL, stderr=subprocess.STDOUT);
    end = datetime.datetime.now()
    
    FNULL.close()
    return (end - start)
    
## Ensure all files and directories are in place before running a pricing.exe
## Calls ModifyInstructions() and RunPricing()
## The 3 parameters are used to find the source files
def ExecutePricing (clientID, programID, model):
    progDir = "prog" + programID
    sourceDir = source + "clnt" + clientID + "\\" + progDir + "\\"
    destDir = root + progDir + "\\" + model
    
    ## ensure the source and destination directories exist
    if not (os.path.exists(sourceDir)):
        ThreadPrint("Error cannot find " + sourceDir)
        return
    
    if not(os.path.exists(destDir + "_run_A")):
        os.makedirs(destDir + "_run_A")
    if not(os.path.exists(destDir + "_run_B")):
        os.makedirs(destDir + "_run_B")
    
    ## copy the files necessary for execution
    fileList = ["layers.txt", "instruction-pricing.txt", "probs-for-allocation.txt", "probs.txt", "probs2.txt"]
    if testImpact:
        fileList.append("instruction-impact.txt")
    
    for file in fileList:
        fileSource = sourceDir + model + "\\" + file
        shutil.copyfile(fileSource, destDir + "_run_A\\" + file)
        shutil.copyfile(fileSource, destDir + "_run_B\\" + file)
    
    ## modify the instruction file(s) so they point to the root
    ModifyInstructions(destDir)
    
    shutil.copyfile(pricingA, destDir + "_run_A\\pricing.exe")
    shutil.copyfile(pricingB, destDir + "_run_B\\pricing.exe")
    
    timeA = None
    timeAImp = None
    timeB = None
    timeBImp = None
    
    ## run the specified executables
    if (numToRun == 1 or numToRun == 3):
        runDir = destDir + "_run_A\\"
        timeA = RunPricing(runDir, 0)
        if testImpact:
            timeAImp = RunPricing(runDir, 1)
    if (numToRun == 2 or numToRun == 3):
        runDir = destDir + "_run_B\\"
        timeB = RunPricing(runDir, 0)
        if testImpact:
            timeBImp = RunPricing(runDir, 1)

    ## add the performance information if any
    result = [programID, model, "Pricing"]
    if timeA == None:
        result.append("0:00:00")
    else:
        result.append(str(timeA))
    if timeB == None:
        result.append("0:00:00")
    else:
        result.append(str(timeB))
    performanceResults.append(result)
    
    if testImpact:
        result = [programID, model, "Impact"]
        if timeAImp == None:
            result.append("0:00:00")
        else:
            result.append(str(timeAImp))
        if timeBImp == None:
            result.append("0:00:00")
        else:
            result.append(str(timeBImp))
        performanceResults.append(result)
    return

## The entry point for all threads
## 'line' is a file path containing a clientID, programID, and a model
def Start():
    try:
        while True:
            line = lines.get(False)
            
            temp = line.split("\\")
            clientID = temp[len(temp) - 3][4:]  ## remove clnt
            programID = temp[len(temp) - 2][4:] ## remove prog
            model = temp[len(temp) - 1]
            info = "Program " + programID + " Model " + model
            
            try:
                ## run the executable
                ThreadPrint("Beginning execution for " + info + "\n")
                ExecutePricing(clientID, programID, model)
                
                ## compare the output.xml files
                ThreadPrint("Beginning output comparison for " + info + "\n") 
                RunXMLCompare(programID, model)
                
                ## compare the payoff files
                ThreadPrint("Beginning payoff comparison for " + info + "\n")
                csvCompare(programID, model)
            except Exception:
                sys.stderr.write("Critical error for " + info + "\n")
                traceback.print_exc()
                lines.task_done()
    except Queue.Empty:
        ## no more work so exit
        pass
    return

## creates a file called Performance_Summary.txt
## contains the runtime of all the executables that were able to run
def CreatePerfSummary():
    ## create the performance results file
    file = "Performance_Summary.txt"
    if not (resultDir == None):
        file = resultDir + file
    perfFile = open(file, "w")
    for result in performanceResults:
        for cell in result:
            perfFile.write(cell + "\t")
        perfFile.write("\n")
    perfFile.close()
    return

## creates a file called Difference_Summary.txt
## by looking at all the summary files in the result directory
## nothing will happen if a result directory is not given(-r)
def CreateDiffSummary():
    if resultDir == None:
        return

    temp = resultDir + "Difference_Summary.txt"
    summFile = open(temp, "w")
    summFile.write("Program Id\tModel\tRisk Groups\tFields\n")
    for file in os.listdir(resultDir):
        if fnmatch.fnmatch(file, "summary_prog*.txt"):
            diffFile = open(resultDir + file, "r")
            
            ## extract the information from the top 4 lines
            programID = diffFile.readline().rstrip("\n").split("\t")[1]
            model = diffFile.readline().rstrip("\n").split("\t")[1]
            yn = diffFile.readline().rstrip("\n").split("\t")
            riskGroups = diffFile.readline().rstrip("\n").split("\t")[1]
            
            if len(yn) > 1 and yn[1] == "Yes":
                summFile.write(programID + "\t" + model + "\t" + riskGroups + "\t")

                ## create a list of the fields with differences
                for line in diffFile:
                    field = line.rstrip("\n").split("\t")
                    if not(field[1] == ""):
                        summFile.write(field[0] + " " + field[1] + "; ")
                summFile.write("\n")
            diffFile.close()  
    summFile.close()        
    return
  
## read the input string for flags and values
## sets/changes global variables  
def ParseArgs():
    global pricingA
    global pricingB
    global inputFile
    global testImpact
    global numToRun
    global limit
    global runningSemaphore
    global resultDir
    
    try:
        offset = 0
        args = sys.argv
        for i in range(0, len(args)):
            if args[i] == "-t":
                testImpact = 1
                offset += 1
            elif args[i] == "-e":
                numToRun = int(args[i+1])
                if not(0 <= numToRun <= 3):
                    sys.stderr.write("Invalid number given for -e")
                    quit()
                offset += 2
                i += 1
            elif args[i] == "-l":
                limit = int(args[i+1])
                offset += 2
                i += 1
            elif args[i] == "-r":
                resultDir = args[i+1]
                if not (resultDir.endswith("\\")):
                    resultDir += "\\"
                offset += 2
                i += 1
            elif args[i] == "-h":
                sys.stderr.write(HELP)
                quit()
            elif args[i].startswith("-"):
                sys.stderr.write("Unrecognised flag: " + args[i])
                quit()
        inputFile = open(sys.argv[1+offset], "r")
        pricingA = sys.argv[2+offset]
        pricingB = sys.argv[3+offset]
        runningSemaphore = threading.BoundedSemaphore(value=limit)
    except:
        traceback.print_exc()
        sys.stderr.write(HELP)
        quit()
    return offset

## ensures that all the necessary files exists
def CheckPaths():
    if not(os.path.exists(pricingA)):
        sys.stderr.write(pricingA + " was not found")
    elif not(os.path.exists(pricingB)):
        sys.stderr.write(pricingB + " was not found")
    elif not(os.path.isfile("PricingCompare.exe")):
        sys.stderr.write("PricingCompare.exe was not found")
    elif not(os.path.isfile("zscore.exe")):
        sys.stderr.write("zscore.exe was not found")
    else:
        try:
            if not(resultDir == None) and not(os.path.isdir(resultDir)):
                os.mkdir(resultDir)
            ## if it makes it here everything checks out
            return
        except Exception:
             sys.stderr.write("The specified result path is invalid")
    ## something went wrong
    quit()
    
def Main():
    try:
        ParseArgs()
        CheckPaths()
        for line in inputFile:
            line = line.rstrip("\n")
            lines.put(line)
        inputFile.close()
        
        allThreads = []
        
        for i in range(limit): 
            t = threading.Thread(target = Start)
            t.start()
            allThreads.append(t)
            time.sleep(1)
        
        ## wait for all the threads to finish
        for aThread in allThreads:
            aThread.join()
        
        ## create the summary files
        if numToRun > 0:
            CreatePerfSummary()
        CreateDiffSummary()

        print("Finished\n")
    except:
        sys.stderr.write("Critical error encountered")
        traceback.print_exc()
        quit()
    return

Main()
