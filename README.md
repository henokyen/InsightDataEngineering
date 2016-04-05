# InsightDataEngineering
A python solution to the coding challenge from Insight Data Engineering Program

Challenge Summary 
The challenge is to calculate the average degree of a vertex in a Twitter hashtag graph for the last 60 seconds, and update this each time a new tweet appears. Thus, calculating the average degree over a 60-second sliding window. The Twitter hashtag graph is a graph connecting all the hashtags that have been mentioned together in a single tweet. More details are available in the official repository: https://github.com/InsightDataScience/coding-challenge.
Solution summary
The graph is represented by two python dictionaries: dictionary of hashtags and dictionary of edges. Hashtags are end-points of edges. The edge dictionary stores edges as a min-heap to efficiently evict tweets that are older than a new tweet by more than 60 secs. Comments made to the code give more details of the solution.
Solution has been tested using tweets.txt (10,000 tweets provided in the data-gen folder), and using  additional tweets downloaded via Twitter API. It can process a minute of tweets in less than a minute.
To run the solution on a Linux/UNIX system, simply execute the run.sh scrip from the top-level directory.
Dependencies 
Python 2.7.3
priority_dictionary.py (originally obtained from: https://gist.github.com/matteodellamico/4451520). This module is slightly modified to make it work for this challenge and is included with this submission. 
 


