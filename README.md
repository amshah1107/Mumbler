# Mumbler
Mumbler application to search keywords based on frequency

# Project Description
The google ngram viewer consists of a dataset that is generated of ngrams from the scanning of the books. The dataset and information can be found in the link below. 
http://storage.googleapis.com/books/ngrams/books/datasetsv2.html

We are using the 2gram dataset for this project. The data is arranged in 99 zip files totalling 27 GB of data. Each zip file contains a tsv file with the following format. 

ngram TAB year TAB match_count TAB volume_count NEWLINE

for example, 
aperture fits 1979  46  91

Each tsv file consists of approximately 60M lines

# Platform

IBM cluster platform of 3 VMs (2 CPUS, 4GB Ram, 25GB disk1, 25GB disk2). One of the servers, named gpfs1 is the quorrum as shown below. 

![Machine Environment](https://github.com/amshah1107/Mumbler/blob/master/machine_envionment.png)

The application is written in python and can be started on any node. The node on which the program is started is called the master node and the others are designated as slaves. The slave just processes the zip files and creates the dictionary. 

# Solution

Each VMs are assigned 33 files to process. URL of each file is opened and unzipped in memory. Each line of the tsv file is read and 2gram and match count are extracted and stored in the dictionary in each node with the dictionary begining with the first letter.  

Once all the VMs are done processing the files, the dictionaries are copied to the common folder and concatenated into a master_dict. This combines the match count of the 2grams found on different documents in the VM. 

After the concatatination is completed, a mumbler_dict is created where the key is the first word of teh 2gram and the value is a dictionary with the second word as the key and value as the match count. 

The word to search is then looked up in the mumbler_dict. If the word is found then a random second word is selected and the second word search begins like the first word. The words are searched until no more 2grams words are found or the max number of words are reached. 

# output

The output of the program is as follows:
[output file](output.txt)


