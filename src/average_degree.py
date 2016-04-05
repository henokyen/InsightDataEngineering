'''
Created on Mar 30, 2016

@author: henok
'''

import json
import fileinput
import time
from datetime import datetime
import sys
import itertools
from priority_dictionary import priority_dict

"""
 An edge class that represents edges in the Twitter-hashtag graph. 
 Edge objects are keys in a dictionary, therefore this class defines the __hash__() __eq__() methods,
 i.e., for edge objects that __eq__ compares equal, __hash__ returns the same hash value.
 """
class Edge (object):  
    """ Initializes an edge object with an origination-end (HashTagFrom) and the other-end (HashTagTo).
    """
    def __init__ (self,HashTagFrom, HashTagTo):   
        self.HashTagFrom=HashTagFrom
        self.HashTagTo=HashTagTo
    
    """ Returns the origination-end and the other-end of an edge 
    """        
    def getHashTags(self):    
        return self.HashTagFrom,self.HashTagTo
    
    def __str__(self):
        return self.HashTagFrom+"->"+self.HashTagTo
    
    """ A method to identify identical edges. Since, the twitter hashtag graph is undirected graph edges do not have direction. 
        For example edges sp->ap and ap->sp are equal and should have the same hash value.
    """
    def __eq__(self,other):       
        if (self.HashTagFrom == other.HashTagTo and other.HashTagFrom == self.HashTagTo):
           return True; 
        elif (self.HashTagFrom == other.HashTagFrom and other.HashTagTo == self.HashTagTo):
            return True
        else: return False
    
    """ Combine the two end-points of an edge, sort the resulting string and then hash it.                    
         """     
    def __hash__(self):          
        
        CompinedHashtags = ''.join(sorted(self.HashTagFrom+self.HashTagTo))
        return hash(CompinedHashtags)
    """
This class represents the Twitter-hashtag graph as a collection of Hashtags and edges.    
hash_tagset is a dictionary with hashtags as keys and their respective degrees as values.
dict_edge_set is also a another dictionary with edges as keys and their creation times as values. 
In the dict_edge_set dictionary, edges are prioritized based on their creation time, i.e., edges come out in order of their creation time,
This priority queue is a min-heap priority queue, which is implemented in priority_dictionary.py (Originally from: https://gist.github.com/matteodellamico/4451520). 
This module is slightly modified to make it work for this challenge.  

The Eviction method evicts tweets (also edges and hashtags) older than 60 seconds from the maximum timestamp being processed. 
Each Hashtag (i.e., end-points) of an evicted edge will have its degree reduced by one and if that reduction results in 0, then that hashtag will be deleted from the graph.

The Output method writes the value of the rolling_average to a rolling_output file only if the size of the hash_tagset is non-zero. Otherwise it writes the value 0.00    

The ProcessTweet method parses each tweet and builds the Twitter-hashtag graph. The method extract unique hashtags and use them to form edges. 
Based on their creation times, edges and hashtags are added into the graph.
"""   
class HashTagGraph(object):   
    hash_tagset = {} 
    rolling_average = 0.0   
    number_of_hashtags = 0
    number_of_edges = 0    
    creation_time = datetime.min # creation time of tweets
    earliest_time = datetime.max # earliest timestamp processed 
    latest_time =   datetime.min # latest timestamp processed
    DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"
    dict_edge_set = {} 
    def __init__(self):        
        self.dict_edge_set = priority_dict() 
        self.hash_tagset = {}  
    def Eviction(self,creation_time):        
        for i in range (0, len(self.dict_edge_set.keys())):
            older_tweet = self.dict_edge_set.smallest()
            if (creation_time -older_tweet[1]).total_seconds() > TIME_WINDOW:                
                end_points = older_tweet[0].getHashTags()
                if (end_points[0] in self.hash_tagset): # sanity check
                    self.hash_tagset[end_points[0]] -=1
                    if self.hash_tagset[end_points[0]]==0: self.hash_tagset.pop(end_points[0], None) #remove the hashtag if it is no longer connected
                if (end_points[1] in self.hash_tagset): # sanity check
                    self.hash_tagset[end_points[1]] -=1
                    if self.hash_tagset[end_points[1]]==0: self.hash_tagset.pop(end_points[1], None) #remove the hashtag if it is no longer connected
                self.dict_edge_set.pop_smallest() 
            else:                 
                self.earliest_time = older_tweet[1]
                break                                 
    """
     Creates edges and hashtags only from tweets that arrive in order in time or 
     from tweets that arrive out of order in time, but fall within the 60 sec window of the maximum timestamp processed.              
     """    
    def ProcessTweet(self, Tweet):           
            self.creation_time = datetime.strptime(Tweet['created_at'],HashTagGraph.DATE_FORMAT)         
            if (self.latest_time < self.creation_time or ((self.latest_time - self.creation_time).total_seconds() < TIME_WINDOW)):                              
                """ collect unique hashtags from a tweet and return it as a list.Hence, no self connection would exist in the graph """
                UniqueHashtags = list(set([x["text"] for x in Tweet['entities']['hashtags']])) 
                hashtags_count = len(UniqueHashtags) 
        
                if hashtags_count >= 2:                  
                    # pair the hashtags to form edges 
                    PairHashtag = [(UniqueHashtags[i],UniqueHashtags[j]) for i in range(hashtags_count) for j in range(i+1, hashtags_count)]                
                    for Pair in PairHashtag:
                        """ 
                        Create an edge and add it to the graph.Then, the individual end-points of the edge are also added to the graph
                        """                    
                        Ed = Edge(Pair[0],Pair[1])                    
                        self.dict_edge_set[Ed] = self.creation_time                       
                        if (Pair[0] in self.hash_tagset):self.hash_tagset[Pair[0]] +=1
                        else:self.hash_tagset[Pair[0]] =1
                        if (Pair[1] in self.hash_tagset):self.hash_tagset[Pair[1]] +=1
                        else:self.hash_tagset[Pair[1]] =1                                    
                    """ 
                    now that the graph has changed, update the Latest and Earliest times                
                    """ 
                    if (self.latest_time < self.creation_time): self.latest_time = self.creation_time
                    if (self.earliest_time > self.creation_time): self.earliest_time = self.creation_time                          
                    """ If a tweet contains only one hashtag, then that hashtag can't be added to the graph. 
                    But it can trigger evicttion prior tweets based on its creation time, so update the maximum timestamp processed 
                    """
                elif hashtags_count == 1:
                    self.creation_time = datetime.strptime(Tweet['created_at'],HashTagGraph.DATE_FORMAT)
                    """
                     If a tweet is empty, then set creation time to 
                     the previous maximum timestamp, so that the tweet won't be considered in building the graph
                    """   
                else: self.creation_time = self.latest_time        
    
    def Output(self,rolling_output):      
        if (len(self.hash_tagset) !=0):
            self.rolling_average = float(2 * len(self.dict_edge_set))/float(len(self.hash_tagset))
            rolling_output.write("%.2f" % (int(self.rolling_average*100)/float(100)))
        else:rolling_output.write("%1.2f" % 0.00) # If there are no connections for the entire graph, then the rolling average is 0.00 
        rolling_output.write('\n')
if __name__ == '__main__':   
    """
    Definition and initialization of program parameters. 
    """
    tweet_count = 0    
    TIME_WINDOW = 60
    twitter_graph = HashTagGraph() #Initialize a Twitter-hashtag graph object
    tweet_source = sys.argv[1]
    rolling_output = open(sys.argv[2],'w')
    start_time = time.clock()   
    """
    This is the main loop.
     For each individual tweet 
      call the ProcessTweet method
      if needed, evict older tweets (i.e., update the graph)    
      write the current value of the Rolling average of the graph to an output file 
    """    
    for line in fileinput.input(tweet_source):               
        Tweet = json.loads(line) # load a single tweet as Python dict        
        if 'limit' in Tweet: continue # rate-limit inputs are ignored 
        twitter_graph.ProcessTweet(Tweet)
        """This condition is always false for empty tweets and for tweets which are out of order in time and are outside the 60-second window,
            otherwise perform eviction  """                   
        if ((twitter_graph.creation_time -twitter_graph.earliest_time).total_seconds() ) > TIME_WINDOW:
            twitter_graph.Eviction(twitter_graph.creation_time)  
        twitter_graph.Output(rolling_output) 
        tweet_count = tweet_count + 1  
       
    print "It takes", time.clock() - start_time, "seconds to process ",  tweet_count, " tweets"     