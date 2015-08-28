import xml.etree.ElementTree as ET
import sys
import traceback
import os
import Queue
import threading
import time
import subprocess
programs = Queue.Queue()

HELP = """NAME
    PriceCompare.py

SYNOPSIS
    PriceCompare.py [OPTION]

OPTION
    -h 
        Print this message and exits
        
    -p  [programs]
        Input a program in format prog54323\Validus or prog[#]\[model] to run a comparison of that program
        
    -f  [Files]
        Input a file to perform a list of comparisons
        
    -r  [Run location]
        Path of run location
        
    -t  [Output location]
        Path of summary file outputs
        
    -z  
        Prevents zscore payoff comparisons from running
        
    -d  [Difference Tolerance]
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
        
    Run Folder Location is the folder location of outputted pricing results
        If the (impact|pricing)-output.xml files are in 
            E:\work\Edison\pricingtest\prog48622\RMS_run_B
        then Run Folder Location is 
            E:\work\Edison\pricingtest
            
    Output Folder is self explanatory. 
        It will contain a summary program of runs A,B and differences summary between them
        Will also contain differences report for all the runs
        
"""


#Initialize Global Variables
inputFile = None
run_location = None
output_location = None
zscore_off = None
difference_tolerance = 10000
instruction_lines = []
printLock = threading.Lock()
errorLock = threading.Lock()
errorout = []
#ThreadPrint()
#Prevents multiple Prints per Line
#
#
def ThreadPrint(out):
    with printLock:
        print (out)
    return
    
def ErrorPrint(out):
    with errorLock:
        try:
            errorout.append(out)
        except Exception:
            traceback.print_exc()
            programs.task_done()
            quit()
    return
#Start()
#Calls the other functions
#Multithreaded by main
#
def Start():
    try:
        while True:
            line = programs.get(False)
            temp = line.split("\\")
            program = temp[len(temp) - 2]
            model = temp[len(temp) - 1]
            info = program + "   |   " + model
            difference_count = 0
            try:
                program_run_folder = run_location+"\\"+program+"\\"+model
                _error = ""
                if not zscore_off:
                    diff = CSVCompare(program[4:], model)
                    
                    if diff:
                        ThreadPrint(info +" Payoff Differences: " + str(diff))
                    
                
                #Want to ensure the files are present for comparison and check pricing outputs
                if os.path.exists(program_run_folder+"_run_A\\"+"pricing"+"-output.xml") and os.path.exists(program_run_folder+"_run_B\\"+"pricing"+"-output.xml"):
                    _resultsA,_resultsB,contracts,fields = ParseXML(run_location+"\\"+program+"\\"+model,0 )
                    diffs,diff_fields,_error = CreateDiff(_resultsA,_resultsB,contracts,fields)
                    difference_count += PrintDiffs(diffs,diff_fields,output_location+"\\"+"diff_pricing"+"_"+program+"_"+model+".txt")
                    PrintSumms(_resultsA,contracts,fields,output_location+"\\"+program+"_"+model+"_runA_pricing"+".txt")
                    PrintSumms(_resultsB,contracts,fields,output_location+"\\"+program+"_"+model+"_runB_pricing"+".txt")
                else:
                    with open(output_location+"\\_error-report.txt","a") as errors:
                        if not os.path.exists(program_run_folder+"_run_A\\"+"pricing"+"-output.xml"):
                            errors.write(program_run_folder+"\tRun A Pricing Output Error\n")
                            ThreadPrint (program+"\t" + model+"      \tRun A Pricing Output not Found")
                        elif not os.path.exists(program_run_folder+"_run_B\\"+"pricing"+"-output.xml"):
                            errors.write(program_run_folder+"\tRun B Pricing Output Error\n")
                            ThreadPrint (program+"\t" + model+"      \tRun B Pricing Output not Found")
                        
                if _error:
                    ThreadPrint( info + " Pricing: "+_error)
                    ErrorPrint( info + " Pricing: "+_error)
                #Do it again for impact
                if os.path.exists(program_run_folder+"_run_A\\"+"impact"+"-output.xml") and os.path.exists(program_run_folder+"_run_B\\"+"impact"+"-output.xml"):                    
                    _resultsA,_resultsB,contracts,fields = ParseXML(run_location+"\\"+program+"\\"+model,1 )
                    diffs,diff_fields,_error = CreateDiff(_resultsA,_resultsB,contracts,fields)
                    difference_count += PrintDiffs(diffs,diff_fields,output_location+"\\"+"diff_impact"+"_"+program+"_"+model+".txt")
                    PrintSumms(_resultsA,contracts,fields,output_location+"\\"+program+"_"+model+"_runA_impact"+".txt")
                    PrintSumms(_resultsB,contracts,fields,output_location+"\\"+program+"_"+model+"_runB_impact"+".txt")
                else:
                    with open(output_location+"\\_error-report.txt","a") as errors:
                        if not os.path.exists(program_run_folder+"_run_A\\"+"impact"+"-output.xml"):
                            errors.write(program_run_folder+"\tRun A impact Output Error\n")
                            ThreadPrint (program+"\t" + model+"      \tRun A Impact Output not Found")
                        elif not os.path.exists(program_run_folder+"_run_B\\"+"impact"+"-output.xml"):
                            errors.write(program_run_folder+"\tRun B impact Output Error\n")
                            ThreadPrint (program+"\t" + model+"      \tRun B Impact Output not Found")
                if _error:
                    ThreadPrint( info + " Impact: "+_error)
                    ErrorPrint( info + " Impact: "+_error)
                if difference_count:
                    ThreadPrint( info + ":    \t"+str(difference_count)+"\tDifferences Found")
 
            except Exception:
                sys.stderr.write("Critical error for " + line + "\n")
                traceback.print_exc()
                programs.task_done()
    except Queue.Empty:
        ## no more work so exit
        pass
    return

  
def CSVCompare(programID, model):
    payoffList = []
   
    ## A and B should have the same layers file
    runDir = run_location + "\\prog" + programID + "\\" + model
    if os.path.exists(runDir + '_run_A\\layers.txt') and os.path.exists(runDir + '_run_B\\layers.txt'):
        with open(runDir + '_run_A\\layers.txt', 'r') as f:
            f.readline() # Skip header
            for row in f:
                payoffID = row.split()[0]
                payoffList.append(payoffID)
                
    elif not os.path.exists(runDir + '_run_A\\layers.txt'):
        ThreadPrint(runDir + '_run_A\\layers.txt not found')
        return
    elif not os.path.exists(runDir + '_run_B\\layers.txt'):
        ThreadPrint(runDir + '_run_B\\layers.txt not found')
        return

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
        diffFile = "\\diff_prog" + programID + "_" + model + ".txt"
        diffFile = output_location + diffFile
        payoffPath = "\\adjusted\\" + modelInfo + "\\iter-event-for-payoff-contract-" + item + ".txt"
        args = "-F csvCompare -f " + run_location + "\\payoffA" + payoffPath + " -A " + run_location + "\\payoffB" + payoffPath
        args += " -k0-2 -s 1 -c3-6 -b 1 1"
        with open(diffFile, "w") as outFile:
            with open(os.devnull, "w") as errFile:
                subprocess.call("\\\\cavcan03\\work\\Alex\\PricingTest\\pricingtest\\bin\\zscore.exe " + args, stdout=outFile, stderr=errFile)
        
        
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
            f.close

    
    if diff:
        with open(diffFile, "w") as outFile:
            for row in diff:
                i = 0
                for item in row:
                    if i == 0:
                        outFile.write(str(item) + "\t")
                    else:
                        outFile.write(str(item) + "\n")
                    i += 1
    return diff
    
# this functionality is to allow a certain threshold of 
# differences to be accepted. The main cases are
# for differences less than 10^-4 relative error
# also any differences amounting to float rounding errors        
def IsDifferent(a,b):
    if not a:
        a = 0
    if not b:
        b = 0
    #Catch random strings(mostly issues with random strings)
    try:
        diff = abs(float(a)-float(b))
        
        if diff == 0:
            return False
        if abs((float(b)/diff) > difference_tolerance):
            return False
        if diff >= 10:
            return True
            
        # There is an issue with one program that didn't properly cast to float
        # We allow a bit of leeway
        # This part tries to determine if the difference is a result of a rounding error
        while diff - 1 < (-0.001) :
            diff = diff * 10
      
        if abs(diff - 1) < (0.001):
            return False
        else: 
            return True
    except ValueError:
        return False
    
        
#CreateDiffs()
#Creates differences from _resultsA and _resultsB
#populates global diffs dictionary and diff_fields
def CreateDiff(_resultsA,_resultsB,contracts,fields):
    diffs = {}
    diff_fields = []
    _error_b = ""
    _error_a = ""
    _error = ""    
    for contract in contracts: 
        if not contract in _resultsA:
            _error  = "contract "+contract+" not found in _resultsA"
            return diffs, diff_fields, _error 
        elif not contract in _resultsB:
            _error  = "contract "+contract+" not found in _resultsB"
            return diffs, diff_fields, _error 
        else: 
            for riskgroup in _resultsA[contract]["riskgroups"]:
                if not riskgroup in _resultsB[contract]:
                    _error_b +=riskgroup +", "
                else:
                    for field in fields:
                        if field in  _resultsA[contract][riskgroup]:                    
                            if IsDifferent( _resultsA[contract][riskgroup][field], _resultsB[contract][riskgroup][field]):
                                if not contract in diffs:
                                    diffs[contract] = {}
                                if not riskgroup in diffs[contract]:
                                    diffs[contract][riskgroup] = {}
                                diffs[contract][riskgroup][field] = str(_resultsA[contract][riskgroup][field])+"/"+str(_resultsB[contract][riskgroup][field])
                                if not field in diff_fields:
                                    diff_fields.append(field)
        for riskgroup in _resultsB[contract]["riskgroups"]:
            if not riskgroup in _resultsA[contract]:
                _error_a += riskgroup + ", "
                
    if _error_a:
        _error = "Field Error: " + _error_a.strip(", ") + " : Were found in run B but not A"
        if _error_b:
            _error += "\nAnd: " +_error_a.strip(", ")+ " : Were found in run A but not B"
    elif _error_b:
        _error = "Field Error: " + _error_b.strip(", ") + " : Were found in run A but not B"
    return diffs, diff_fields, _error
                            
                    
#ParseXM() 
#Interprets the vertical XML files into dictionaries
#Populates Contract and Field Lists                                      
def ParseXML(file,impact):
    contracts = []
    fields = []
    _resultsA = {}
    _resultsB = {}
    
    file_path = file + "_run_A\\"+ ("impact" if impact else "pricing")+"-output.xml"
    
    tree = ET.parse(file_path)
    root = tree.getroot()
    for child in root.findall("{http://vcaps3/web}contractresult"):
        if child[1].tag == "{http://vcaps3/web}contract_id":
            contract_id  = child[1].text
        else:
            contract_id  = child[0].text
        if not contract_id in contracts:
            contracts.append(contract_id) 
        _resultsA[contract_id] = {}
        _resultsA[contract_id]["riskgroups"] = []
        for zoneresult in child.findall("{http://vcaps3/web}zoneimpactresult" if impact else "{http://vcaps3/web}zonepricingresult"):
            _resultsA[contract_id][zoneresult[0].text] = {}
            _resultsA[contract_id]["riskgroups"].append( zoneresult[0].text)
            for field in zoneresult[1:]:
                _resultsA[contract_id][zoneresult[0].text][field.tag[19:]] = field.text
                if (not field.tag[19:] in fields) and (not field == "portTitle"):
                    fields.append(field.tag[19:])
    file_path = file + "_run_B\\"+ ("impact" if impact else "pricing")+"-output.xml"
    tree = ET.parse(file_path)
    root = tree.getroot()
    for child in root.findall("{http://vcaps3/web}contractresult"):
        if child[1].tag == "{http://vcaps3/web}contract_id":
            contract_id  = child[1].text
        else:
            contract_id  = child[0].text
        if not contract_id in contracts:
            contracts.append(contract_id)  
        _resultsB[contract_id] = {}
        _resultsB[contract_id]["riskgroups"] = []
        for zoneresult in child.findall("{http://vcaps3/web}zoneimpactresult" if impact else "{http://vcaps3/web}zonepricingresult"):
            _resultsB[contract_id][zoneresult[0].text] = {}
            _resultsB[contract_id]["riskgroups"].append( zoneresult[0].text)
            for field in zoneresult[1:]:
                _resultsB[contract_id][zoneresult[0].text][field.tag[19:]] = field.text
    return _resultsA,_resultsB,contracts,fields
        
def PrintDiffs(tree,diff_fields,file):
    first_line = ""
    body = ""
    diff_counts = {}
    riskgroups = []
    diff_counts["total"] = 0
    for field in diff_fields:
        diff_counts[field] = 0
    for contract in tree:
        for riskgroup in tree[contract]:
            if not first_line:
                first_line = "contract_id\triskgroup\t"
                for field in diff_fields:
                    first_line += field+"\t"
                first_line += "\n"
            if not riskgroup in riskgroups:
                riskgroups.append(riskgroup)
            body += contract+"\t"+riskgroup+"\t"
            for field in diff_fields:
                if field in tree[contract][riskgroup]:
                    body += tree[contract][riskgroup][field]+"\t"
                    diff_counts[field] +=1
                    diff_counts["total"] +=1
                else:
                    body += " \t"
            body +="\n"
    
    if body:
        body += " \t"
        for riskgroup in riskgroups:
            body+= riskgroup + " "
        body += " \t"
        for field in diff_fields:
            body += str(diff_counts[field]) + " \t"
        with open (file, "w+") as outfile:
            outfile.write(first_line)
            outfile.write(body)
        return diff_counts["total"]
    else:
        if os.path.exists(file):
            os.remove(file)
        return 0
    

## creates a file called _Difference_Summary.txt
## by looking at all the summary files in the result directory
## nothing will happen if a result directory is not given(-r)
def CreateDiffSummary():

    body = ""
    for line in instruction_lines:
        temp = line.split("\\")
        program = temp[len(temp) - 2]
        model = temp[len(temp) - 1]
        diffFile = "\\diff_prog" + program[4:] + "_" + model + ".txt"
        diffFile = output_location + diffFile
        if os.path.exists(diffFile):
            if not os.stat(diffFile).st_size:
                os.remove(diffFile)
        
        if os.path.exists(output_location +"\\"+"diff_pricing"+"_"+program+"_"+model+".txt"):
            body += program[4:]+"\t"+model+"\t"
            with open (output_location +"\\"+"diff_pricing"+"_"+program+"_"+model+".txt", "r") as diffFile:
                first_line = diffFile.readline().strip("\t\n").split("\t")[2:]
                for last_line in diffFile:
                    pass
                last_line = last_line.strip("\t\n").split("\t")
                body+= last_line[1] + "\t"
                total = 0
                for i in range(len(first_line)):
                    body += first_line[i]+" "+last_line[i+2]+";"
                    total += int(last_line[i+2])
                body +="\t" + str(total)+"\t"
            if os.path.exists(output_location +"\\"+"diff_impact"+"_"+program+"_"+model+".txt"):
                with open (output_location +"\\"+"diff_impact"+"_"+program+"_"+model+".txt", "r") as diffFile:
                    first_line = diffFile.readline().strip("\t\n").split("\t")[2:]
                    for last_line in diffFile:
                        pass
                    last_line = last_line.strip("\t\n").split("\t")
                    body+= last_line[1] + "\t"
                    total = 0
                    for i in range(len(first_line)):
                        body += first_line[i]+" "+last_line[i+2]+";"
                        total += int(last_line[i+2])
                    body +="\t" + str(total)+"\t"   
            else:
                body += "N\\A\tN\\A\tN\\A\t"
            body +="\n"
        elif os.path.exists(output_location +"\\"+"diff_impact"+"_"+program+"_"+model+".txt"):
            body += program[4:]+"\t"+model+"\tN\\A\tN\\A\tN\\A\t"
            with open (output_location +"\\"+"diff_impact"+"_"+program+"_"+model+".txt", "r") as diffFile:
                first_line = diffFile.readline().strip("\t\n").split("\t")[2:]
                for last_line in diffFile:
                    pass
                last_line = last_line.strip("\t\n").split("\t")
                body+= last_line[1] + "\t"
                total = 0
                for i in range(len(first_line)):
                    body += first_line[i]+" "+last_line[i+2]+";"
                    total += int(last_line[i+2])
                body +="\t" + str(total)+"\t"
            body +="\n"
    with open ( output_location + "\\_Difference_Summary.txt","w+") as outfile:
        outfile.write("Program Id\tModel\tPricing Diff RG\tPricing Fields\tPricing Totals,\tImpact Diff RG\tImpact Fields\tImpact Totals \n")
        outfile.write(body)
            
   
            
def PrintSumms(tree,contracts,fields,file):
    first_line = "contract_id\t"
    second_line = "riskgroup\t"
    body = ""
    for contract in contracts:
        if contract in tree:
            for riskgroup in tree[contract]["riskgroups"]:
                first_line += contract
                first_line += "\t"
                second_line += riskgroup
                second_line += "\t"
    for field in fields:
        body += field +"\t"
        for contract in contracts:
            if contract in tree:
                for riskgroup in tree[contract]["riskgroups"]:
                    if not field in tree[contract][riskgroup]:
                        body += "\t"
                    else:
                        body += str(tree[contract][riskgroup][field])+ "\t"
        body += "\n"
    with open (file,"w+") as infile:
        infile.write(first_line + "\n")
        infile.write(second_line + "\n")
        infile.write(body)
                
                
                
## read the input string for flags and values
## sets/changes global variables  
def ParseArgs():

    global run_location
    global output_location
    global inputFile
    global instruction_lines
    global zscore_off
    global difference_tolerance

    args = sys.argv
    try:
        if len(args) < 4:
             raise ValueError("Incorrect number of Arguments")
        for i in range(len(args)):
            if args[i] == "-h":
                print (HELP)
                quit()
            elif args[i] == "-f":
                i+=1
                inputFile = args[i]
            elif args[i] == "-p":
                i+=1
                instruction_lines.append(args[i])
                programs.put(args[i])
            elif args[i] == "-r":
                i+=1
                run_location = args[i]
            elif args[i] == "-t":
                i+=1
                output_location = args[i]
            elif args[i] == "-z":
                zscore_off = 1
            elif args [i] == "-d":
                i += 1
                difference_tolerance = args[i]
        try:
            if float(difference_tolerance) == 0:
                difference_tolerance = 99999999999
            else: 
                difference_tolerance = 1 / float(difference_tolerance)
        except ValueError:
            print "-d must be followed by a valid float"
            quit()
            
        if not(os.path.exists(run_location)):
            raise ValueError(run_location+" not Found")
            
        if not(os.path.exists(output_location)):
            os.makedirs(output_location)
        if inputFile:
            if not(os.path.exists(inputFile)):
                raise ValueError(inputFile+" not Found")
            with open(inputFile, "r") as infile:
                for line in infile.readlines():
                    line = line.rstrip("\n")
                    instruction_lines.append(line)
                    programs.put(line)
        with open (output_location+"\\_error-report.txt", "w+") as errors:
            errors.write("Program Folder\tImpact/Pricing \n")
    except ValueError: 
        traceback.print_exc()
        sys.stderr.write(HELP)
        quit()
    
def Main():
    limit = 12
    ParseArgs()
    allThreads = []
    for i in range(limit): 
        t = threading.Thread(target = Start)
        t.start()
        allThreads.append(t)
        time.sleep(1)
    for aThread in allThreads:
        aThread.join()
    CreateDiffSummary()
    with open (output_location+"\\_error-report.txt", "a") as errors:
        errors.write("\n\n Other Errors\n")
        for line in errorout:            
            errors.write(line + "\n")
           
    
    
    


Main()

