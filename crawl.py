import praw
import time
import sys
import json
import os
from prawcore.exceptions import RequestException, TooManyRequests
import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool, Manager
from pathlib import Path

# 1. get page title
def get_page_title(url):
    try:
        # BASE Reddit url + given url
      URL = 'https://reddit.com' + url
      #gets the webpage content from that combined URL
      response = requests.get(URL)
      soup = BeautifulSoup(response.content, "html.parser")
      return soup.title.string
    except Exception as e:
      print(f'Error fetching page title: {e}')
      return None
  
  #We will use mutliple reddit APIs - faster
reddit1 = praw.Reddit(
    client_id = "ehnbGPaaRiU05XUZwCSwAA",
    client_secret = "5YkKlU2Z0BuxVtb-BhfBB12tLrdBgQ",
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
)

reddit2 = praw.Reddit(
    client_id = "TQJsWeYyP3lMhkvPiWJQTg",
    client_secret = "DHfkovUsVLyk1XG183cCFBsSZAZ50g",
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
)

reddit3 = praw.Reddit(
    client_id = "aV5WyYZnLfR2wn97SN4__w",
    client_secret = "quoqq1kCcs4426uJWukB1G7hU1nv3A",
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
)

#the reddits I want to crawl 
reddits = [reddit1, reddit2, reddit3]
#topics I want to crawl 
subredditNames = ['Love Island', 'Gilmore Girls', 'Sephora', 'Tiktok', 'Instagram', 'news', 'books', 'Starbucks']

targetSizeOfData = 100 * 1024 * 1024 

#Tracking crawled IDs - dont want to re-crawl
#file_path = Path('cralwed_ids.json')
# if crawled_ids.json exists. It tries to load the data
# if there's an error, it uses an empty set. 
# If the file doesn't exist, it creates the file with empty list
# if file_path.exists():
#     try:
#         with open(file_path, 'r') as f:
#             crawled_ids = set(json.load(f))
#     except json.JSONDecodeError:
#         crawled_ids = set()
# else:
#     crawled_ids = set()
#     with open(file_path, 'w') as f:
#         json.dump(list(crawled_ids),f)
try:
    with open('crawled_ids.json', 'r') as f:
        crawled_ids = set(json.load(f))
except (FileNotFoundError, json.JSONDecodeError):
    crawled_ids = set()
    with open('crawled_ids.json', 'w') as f:
        json.dump(list(crawled_ids), f)
        
#Type of posts to crawl
postType = ['top', 'new', 'controversial', 'trending', 'hot']

def crawl_subreddit(args):
    #the Reddit instance,
    # the name of the subreddit to crawl
    # a list of IDs that have already been crawled,
    # a dictionary used for managing state
    reddit, subredditNames, crawled_ids, manager_dict = args
    subreddit = reddit.subreddit(subredditNames)
    subredditData = []
    crawled_idsSet = set(crawled_ids)
    #used to save post data 
    fileName = f'{subredditNames}.json'
    #start looping over each type of post 
    for post_type in postType:
        #gets posts from the subreddit - does not limit amount 
        for submission in getattr(subreddit,post_type)(limit =None):
            try:
                if manager_dict['current_data_size'] >= targetSizeOfData:
                    break
                #avoid re-crawling
                if submission.id in crawled_idsSet:
                    continue
                #used to mark IDs as crawled 
                crawled_idsSet.add(submission.id)
                #creating a post_data dictionary 
                post_data = {
                    'subreddit': subredditNames,
                    'title': submission.title,
                    'id': submission.id,
                    'score': submission.score,
                    'url': submission.url,
                    'permalink': submission.permalink,
                    'comments': [],
                    'permalink_text': get_page_title(submission.permalink)

            }
                
                try:
                #this handles the "More comments" option in reddit
                    submission.comments.replace_more(limit=None)
                #iterate over comments 
                    for comment in submission.comments.list():
                        post_data['comments'].append({
                        'body' : comment.body,
                        'score': comment.score
                    })
                
                #handles rate limit
                except RequestException:
                    print("Hit rate limit while fetching comments, sleeping for 60 seconds")
                    time.sleep(60)
                

                #Now we add post data to subreddit data 
                #Add post data dictionary(has comments now to dictionary)
                subredditData.append(post_data)
                manager_dict['current_data_size'] += sys.getsizeof(post_data)
            
                #Save subreddit data 
            
                with open(fileName, 'w') as f:
                    json.dump(subredditData, f, indent = 5)
                    #prints file name and size
                    file_size_mb = os.path.getsize(fileName)/(1024*1024)
                    print(f"{fileName}{file_size_mb:.2f}MB")
                
                #handles rate limit 
            except TooManyRequests:
                print("Hit rate limit, sleeping for 60 seconds")
                time.sleep(60)
        crawled_ids = list(crawled_idsSet)
        
        #Run as the Main script
        if __name__ == "__main__":
            #initialize manager
            with Manager() as manager:
                #basically handles shared data between processes
                #Converts list of crawled IDs into a managed list(can be shared among processes)
                crawled_ids = manager.list(crawled_ids)
                manager_dict = manager.dict()
                manager_dict['current_data_size'] = 0
            
                #Creates a pool of worker processes -  runs tasks in parallel
                with Pool() as p:
                    p.map(crawl_subreddit, [(reddit, subreddit_name, crawled_ids, manager_dict) for reddit in reddits for subreddit_name in subredditNames])

        print(f'Total data size: {manager_dict["current_data_size"] / (1024 * 1024)}MB')
