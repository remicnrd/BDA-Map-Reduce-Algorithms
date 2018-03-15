# BDA-Map-Reduce-Algorithms

This repo contains map reduce implementations for the class of Big Data Algorithms @ CentraleSupélec.


## Display CSV
Print in terminal for each row of data certain attributes (here Height and Year). The dataset is a CSV file and available at http://opendata.paris.fr .

## Display compact
In a similar way, displays in terminal information for each row of data (here USAF code, country and elevation). The dataset is a text file available at  https://www1. ncdc.noaa.gov/pub/data/noaa/isd-history.txt .

## TF-IDF
Create an output file containing the TF-IDF scores of each unique word in a set of input documents. A Term Frequency–Inverse Document Frequency score is a numerical statistic that is intended to reflect how important a word is to a document. This method can used for Frequentist word embedding. The formula is the following: 
<a href="https://www.codecogs.com/eqnedit.php?latex=tf-idf_{t,d}&space;=&space;(1&space;&plus;\log&space;tf_{t,d})&space;\cdot&space;\log&space;\frac{N}{df_t}" target="_blank"><img src="https://latex.codecogs.com/gif.latex?tf-idf_{t,d}&space;=&space;(1&space;&plus;\log&space;tf_{t,d})&space;\cdot&space;\log&space;\frac{N}{df_t}" title="tf-idf_{t,d} = (1 +\log tf_{t,d}) \cdot \log \frac{N}{df_t}" /></a>

The result file is ordered decreasingly according to scores so we can observe the main words for our input dataset. Data can be found at http://www.textfiles.com/etext/FICTION/defoe-robinson-103.txt and http://www.textfiles.com/etext/FICTION/callwild .

## Page Rank
Create an output file containing the PageRank score of each user in the Epinion network. The result file is ordered decreasingly according to scores so we can observe the highest scores for our input dataset. Data available at https://snap.stanford.edu/data/soc-Epinions1.html .
