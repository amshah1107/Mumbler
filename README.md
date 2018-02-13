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

IBM cluster platform of 3 VMs (2 CPUS, 4GB Ram, 25GB disk1, 25GB disk2). One of the servers, named gpfs1 is the quorum. 


