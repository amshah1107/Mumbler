import random
import datetime
import re
import sys
import os
import zipfile
import pickle
import socket
import cStringIO
import pathos
import time
import logging
import shutil
import urllib

Dictionary_DIR = "dict"
common_dir = '/gpfs/gpfsfpo/'
master_dict_DIR = common_dir + "master_dict" 
mumbler_dict_DIR = common_dir + "mumbler_dict" 
current_VM_type="master"

fileDistDict = { "gpfs1" : [12,13], "gpfs2" :[33,34], "gpfs3" :[66,67] }


def startProgramOnOtherServers(cur_host):
	logging.debug("In startProgramOnOtherServers")
	for each_hosts in fileDistDict.keys():
		if each_hosts == cur_host:
			continue
		logging.debug("starting on " +  each_hosts)
		pathos.core.execute('nohup python mumbler.py slave > mumblerout.txt &',host=each_hosts)
		

def processMachineFiles(machineIdx, letterDictionary, isFileDownloaded):
	logging.debug("In processMachineFiles")
	for idx in range(fileDistDict[machineIdx][0],fileDistDict[machineIdx][1]): 
		start_time = time.time()
		getFile(idx,machineIdx,letterDictionary, isFileDownloaded)
		logging.debug("completed getFile")
		timetaken = str(time.time() - start_time)
		logging.debug( "Time for processing the file:" + timetaken)
	logging.debug("completed processMachineFiles")

def copyDictionaryToCommonFolder(machineIdx):
	logging.debug("In copyDictionaryToCommonFolder")
	tempZipFile = os.getcwd() +  "/" + machineIdx + "_dict.zip"
	finalZipFile = common_dir + machineIdx + "_dict.zip"
	logging.debug("tempzipfile=" +  tempZipFile)
	logging.debug("finalZipFile="+finalZipFile)
	zipdir(Dictionary_DIR, tempZipFile)
	shutil.copyfile(tempZipFile,finalZipFile)
	unzipdir(finalZipFile, common_dir)
	logging.debug("completed copyDictionaryToCommonFolder")

def zipdir(path, zipfileName):
    ziph = zipfile.ZipFile(zipfileName, 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))
    ziph.close()


def unzipdir(path_to_zip_file, directory_to_extract):
	ziph = zipfile.ZipFile(path_to_zip_file, 'r')
	ziph.extractall(directory_to_extract)
	ziph.close()


def createMumblerDict(master_dict,cur_host):
	logging.debug("In createMumblerDict")
	logging.debug("Start time:" + str(datetime.datetime.now()))
	mumbler_dict = makedictionaries()
	for each_word_dict in master_dict:
		lettr = each_word_dict
		for each_word in master_dict[lettr]:
			ngram_words = each_word.split(' ')
			firstWord = ngram_words[0]
			secondWord = ngram_words[1]
			match_count = master_dict[lettr][each_word]
			if not (firstWord in mumbler_dict[lettr]):
				mumbler_dict[lettr][firstWord] = {}
			mumbler_dict[lettr][firstWord][secondWord] = match_count
			#logging.debug("first Word = " +  firstWord + " second word= " + secondWord + " count = " + str(match_count) + " len = " + str(len(mumbler_dict[lettr][firstWord])))

	saveDictionaries(mumbler_dict,mumbler_dict_DIR, cur_host)
	logging.debug("End time:" + str(datetime.datetime.now()))
	return mumbler_dict

def loadMasterDictionary(cur_host):
	logging.debug("In loadMasterDictionary")
	logging.debug("Start time:" + str(datetime.datetime.now()))
	master_dict = {}
	loadDictionaries(master_dict,master_dict_DIR, cur_host)
	logging.debug("master directory loaded")
	logging.debug("End time:" + str(datetime.datetime.now()))
	return master_dict
	
def loadMumblerDictionary(cur_host):
	logging.debug("In loadMumblerDictionary")
	logging.debug("Start time:" + str(datetime.datetime.now()))
	mumbler_dict = {}
	loadDictionaries(mumbler_dict,mumbler_dict_DIR, cur_host)
	logging.debug("mumbler directory loaded")
	logging.debug("End time:" + str(datetime.datetime.now()))
	return mumbler_dict

def getWordList(mumbler_dict,wordToSearch):
	logging.debug("In getWordList")
	logging.debug("Start time:" + str(datetime.datetime.now()))
	firstLetter = wordToSearch[0]
	match_of_all_secondWord = 0
	return_list = []
	wordMatchCount = []
	if wordToSearch in mumbler_dict[firstLetter]:
		searchWord_dict = mumbler_dict[firstLetter][wordToSearch]
		for secondWord in searchWord_dict:
			return_list.append(wordToSearch + "," + secondWord) 
			wordMatchCount.append(searchWord_dict[secondWord])
			match_of_all_secondWord += searchWord_dict[secondWord]
		
		logging.debug("match of all secondWord=",match_of_all_secondWord)
		for k in range(len(wordMatchCount)):
			wordProbability = float(wordMatchCount[k])/match_of_all_secondWord
			return_list[k] += ", {0:f}".format(wordProbability)
		
	return return_list
			
			
def mumbler(mumbler_dict, wordToSearch, noOfWords):
	words_found = 0
	while (words_found < noOfWords):
		wordList = getWordList(mumbler_dict, wordToSearch)
		if len(wordList) == 0:
			print "no words starting with " + wordToSearch + " found"
			logging.debug("no words starting with " + wordToSearch + " found")
			break
		print "Word,secondWord,probability"
		logging.debug("Word,secondWord,probability")
		for val in wordList:
			print(val)
			logging.debug(val)
		select_word = wordList[random.randint(0,len(wordList)-1)].split(',')[1]
		print("\n\n Selected Word is " + select_word + "\n\n")
		logging.debug("\n\n Selected Word is " + select_word + "\n\n")
		wordToSearch = select_word
		words_found += 1

def waitWhileAllNodesComplete():
	logging.debug("In waitWhileAllNodesComplete")
	max_try = 10
	try_count = 0
	all_done = False
	file_on_nodes = []
	done_flag = []
	for each_host in fileDistDict.keys(): 
		file_on_nodes.append(common_dir + each_host + "_done")
		done_flag.append(False)
	while all_done == False and try_count < max_try:
		for i in range(len(file_on_nodes)):
			if (os.path.isfile(file_on_nodes[i])==True):
				done_flag[i] = True
		all_done = all(done_flag)
		if all_done == True:
			break
		time.sleep(5)
		try_count += 1
	logging.debug("completed waitWhileAllNodesComplete")
		

def collateDictionaries(cur_host):
	logging.debug("in collate dictionaries")
	logging.debug("Start time:" + str(datetime.datetime.now()))
	waitWhileAllNodesComplete()
	master_dict = makedictionaries()
	hosts_dict = {}
	for each_host in fileDistDict.keys():
		hosts_dict[each_host] = {}
		loadDictionaries( hosts_dict[each_host], common_dir + each_host + '_dict', each_host)

	logging.debug("dictionaries loaded")
	common_words = 0
	new_words = 0

	if not os.path.isdir(master_dict_DIR):
		os.makedirs(master_dict_DIR)

	for each_word_dict in hosts_dict[cur_host]:
		lettr = each_word_dict
		for each_word in hosts_dict[cur_host][lettr]:
			master_dict[lettr][each_word] = hosts_dict[cur_host][lettr][each_word]
	logging.debug("copied dictionary of " + cur_host)
	for each_host in fileDistDict.keys():
		logging.debug("processing " + each_host)
		common_words = 0
		new_words = 0
		if each_host == cur_host: 
			continue
		for each_word_dict in hosts_dict[each_host]:
			print lettr
			lettr = each_word_dict
			for each_word in hosts_dict[each_host][lettr]:
				if each_word in master_dict[lettr]:
					master_dict[lettr][each_word] += hosts_dict[each_host][lettr][each_word]
					common_words += 1
				else:
					master_dict[lettr][each_word] = hosts_dict[each_host][lettr][each_word]
					new_words += 1
		logging.debug("common words in host " + each_host + str(common_words))
		logging.debug("new words in host " + each_host + str(new_words))
 
	saveDictionaries(master_dict,master_dict_DIR, cur_host)
	return master_dict

	logging.debug("end time:" + str(datetime.datetime.now()))
	return master_dict
	

def createFolders():
	logging.debug("Dictionary_DIR=" + Dictionary_DIR)
	if not os.path.isdir(Dictionary_DIR):
		os.makedirs(Dictionary_DIR)
	if not os.path.isdir(master_dict_DIR):
		os.makedirs(master_dict_DIR)
	if not os.path.isdir(mumbler_dict_DIR):
		os.makedirs(mumbler_dict_DIR)
		

def makedictionaries():
	main_dict = {}
	for i in range(ord('a'),ord('z')+1):
		main_dict[chr(i)] = {}
	return main_dict
		
def saveDictionaries(dict_to_save, dict_path,machineidx):
	logging.debug("In save Dictionaries")
	for i in range(ord('a'),ord('z')+1):
		ngramDictName = dict_path + "/" + chr(i) + "_" + machineidx
		if (len(dict_to_save[chr(i)]) != 0):
			pickle.dump(dict_to_save[chr(i)], open(ngramDictName, "wb"))		

def loadDictionaries(dictionary_to_load_to, dict_path, machineidx):
	for i in range(ord('a'),ord('z')+1):
		ngramDictName = dict_path + "/" + chr(i) + "_" + machineidx
		if (os.path.isfile(ngramDictName)):
			dictionary_to_load_to[chr(i)]= pickle.load(open(ngramDictName,"rb"))
	

def updateDictionary(keyWord, matchcount,machineidx,letterDictionary):
	keyWord = keyWord.lower()
	if keyWord in letterDictionary[keyWord[0]]:
		letterDictionary[keyWord[0]][keyWord] += int(matchcount)
	else:
		letterDictionary[keyWord[0]][keyWord] = int(matchcount)
	return


def downloadFile(url):
	logging.debug( "in downloadFile")
	try:
		logging.debug(url)
		urlfile = urllib.urlopen(url)
		logging.debug( "opened url")
		zf = zipfile.ZipFile(cStringIO.StringIO(urlfile.read()))
		return zf
	except HTTPError as e:
		logging.debug( "error in downloading file" + e.code)
		return None 
	except URLError as e:
		logging.debug( "error in downloading file" + e.code)
		return None 
			

def getFile(fileIdx,machineidx,letterDictionary, isFileDownloaded):
	url="http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-2gram-20090715-" + str(fileIdx) + ".csv.zip" 
	logging.debug( url)
	csvFileName =  "googlebooks-eng-all-2gram-20090715-" + str(fileIdx)+ ".csv" 
	if (isFileDownloaded == 0):
		logging.debug( "over here")
		zf = downloadFile(url)
		if (zf == None):
			logging.debug( "zipfile is none")
			exit()
		logging.debug( "processing csv")
		for line in zf.open(csvFileName).readlines():
			values = line.split('\t')
			if (re.search(r"^[a-z]{3,} [a-z]{3,}",values[0].lower())):
				updateDictionary(str(values[0]), int(values[2]), machineidx,letterDictionary)

	logging.debug( "processing csv completed")

def printUsage():
	print "\n"
	print "Usage:"
	print "    1:" + __file__ + " wordToSearch NoOfWords "
	print "        To run the complete program including downloading files\n"
	print "    2:" + __file__ + " wordToSearch NoOfWords buildMasterDict"
	print "        To run the program without downloding the file."
	print "        This assumes files are downloaded and the " + Dictionary_DIR + "  dictionary is built"
	print "    3:" + __file__ + " wordToSearch NoOfWords buildMumblerDict"
	print "        To run the program without downloding the file"
	print "        This assumes files are downloaded and the " + master_dict_DIR + " Dictionary is built"
	print "    4:" + __file__ + " wordToSearch NoOfWords runMumbler"
	print "        To run the program without downloding the file"
	print "        This assumes files are downloaded and the " + mumbler_dict_DIR + " Dictionary is built"

        
def downloadFilesAndMakeDictionaries(Dictionary_DIR, cur_host):
	logging.debug("In downloadFilesAndMakeDictionaries")
	logging.debug("Dictionary_DIR="+ Dictionary_DIR)
	logging.debug("cur_host = " + cur_host)
	createFolders()
	logging.debug("after createFolders")
	letterDictionary = makedictionaries()
	logging.debug("after makedictionaries")
	processMachineFiles(cur_host,letterDictionary,0)
	logging.debug("after processMachineFiles")
	saveDictionaries(letterDictionary,Dictionary_DIR,cur_host)
	logging.debug("letterDictionary saved")
	copyDictionaryToCommonFolder(cur_host)
	endFile = open("/gpfs/gpfsfpo/" + cur_host + "_done", "w")
	endFile.close()
	logging.debug("completed in downloadFilesAndMakeDictionaries")
	return letterDictionary
	
def	Master_processAll(cur_host,wordToSearch, noOfWords):
	startProgramOnOtherServers(cur_host)
	downloadFilesAndMakeDictionaries(Dictionary_DIR, cur_host)
	master_dict = collateDictionaries(cur_host)
	mumbler_dict=createMumblerDict(master_dict,cur_host)
	mumbler(mumbler_dict,wordToSearch,noOfWords)
	
def Master_processAfterDownload(cur_host,wordToSearch,noOfWords):
	master_dict = collateDictionaries(cur_host)
	mumbler_dict=createMumblerDict(master_dict,cur_host)
	mumbler(mumbler_dict,wordToSearch,noOfWords)


def Master_processAfterMaster_dict(cur_host,wordToSearch,noOfWords):
		master_dict = loadMasterDictionary(cur_host)
		mumbler_dict=createMumblerDict(master_dict,cur_host)
		mumbler(mumbler_dict,wordToSearch,noOfWords)
	
def Master_processAfterMumbler_dict(cur_host,wordToSearch,noOfWords):
		mumbler_dict = loadMumblerDictionary(cur_host)
		mumbler(mumbler_dict,wordToSearch,noOfWords)
	
if __name__ == "__main__":
	logging.basicConfig(filename='mumbler.log',level=logging.DEBUG)
	logging.debug("Start time:" + str(datetime.datetime.now()))
	
	sysargLen = len(sys.argv)
	cur_host = socket.gethostname().split('.')[0]
	current_VM_type = "master" 
	
	Dictionary_DIR = "/" + cur_host + "_" + Dictionary_DIR
	
	logging.debug("argvlen = " + str(sysargLen))

	if sysargLen == 1:
		printUsage()
		exit()	

	if sysargLen == 2:
		if sys.argv[1] == "slave":
			current_VM_type="slave"
			logging.debug( "it is a slave")
		else:
			logging.debug( "Incorrect Usage")
			print "Incorrect Usage: see Usage()"
			printUsage()
			exit()

	logging.debug("current_VM_type = " + current_VM_type)

	if current_VM_type == "slave":
		logging.debug("in slave processing")
		downloadFilesAndMakeDictionaries(Dictionary_DIR, cur_host)
		logging.debug("after downloadFiles")
		logging.debug("slave finished")
		exit()

	fd1 = open("/gpfs/gpfsfpo/master_" + cur_host,"w")
	fd1.close()

	wordToSearch = sys.argv[1]
	noOfWords = sys.argv[2]

	assert(current_VM_type == "master")
	assert(sysargLen >= 3)

	if sysargLen == 3:
		Master_processAll(cur_host,wordToSearch,noOfWords)
		exit()
			
	assert(sysargLen == 4)

	if sys.argv[3] == "buildMasterDict":
		logging.debug("Running buildMasterDict")
		Master_processAfterDownload(cur_host,wordToSearch,noOfWords)
		exit()

	if sys.argv[3] == "buildMumblerDict":
		logging.debug("Running buildMumblerDict")
		Master_processAfterMaster_dict(cur_host,wordToSearch,noOfWords)
		exit()

	if sys.argv[3] == "runMumbler":
		logging.debug("Running Mumbler")
		Master_processAfterMumbler_dict(cur_host,wordToSearch,noOfWords)
		exit()
	
	logging.debug("End time:" + str(datetime.datetime.now()))
	
