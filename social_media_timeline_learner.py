from socialstats.apps.socialposter.models import Keyword
from social_apis import get_timeline, get_detailed_timeline, get_twitter_account, get_fb_info_posts_statuses, get_linkedin_timeline
from socialstats.apps.socialposter.topicweb import get_key_cluster_broad, get_key_cluster
from socialstats.libs.rake.rakev2 import Rake
from socialstats.libs.rake.rake import Rake as OldRake
from socialstats.apps.socialposter.category_update import categorize_keywords_individual, categorize_keywords, category_scores
from datetime import datetime, timedelta
from url_analysis import possibly_sensitive
import re
from math import log
from common import word_variation_not_in, sanitize

def is_not_in_stoplist(word, provider=False):
    facebook_stoplist = ["facebook"]
    linkedin_stoplist = ["linkedin"]
    twitter_stoplist = ["tweet", "twitter"]
    common_stoplist = ["follow", "official","verified", "trend", "account","profile","drumup","http","report","breaking","headlines", "subscribe", "website","analysis", "updates", "pages", "signed", "personal"]
    stoplist = []
    if provider == 'facebook':
        stoplist = facebook_stoplist + common_stoplist
    elif provider == 'twitter':
        stoplist = twitter_stoplist + common_stoplist
    elif provider in ['linkedin', 'linkedin-oauth2']:
        stoplist = linkedin_stoplist + common_stoplist
    else:
        stoplist = common_stoplist+facebook_stoplist+linkedin_stoplist+twitter_stoplist
    for stop_word in stoplist:
        if stop_word in word:
            return False
    return True


def keyword_filter(keywords, provider,related_keywords=True):
    generated_themes_list = []
    generated_themes_dict = {}
    if keywords:
        max_keyword_value = keywords[0][1]
    
        #print keywords
        
        for word in keywords:
            word_string = word[0]
            if word_string[0] == '@' or word_string[0] == '#':
                word_string = word_string[1:]
            if len(word_string) <= 4 or word_string == "social":  #One specific case that occurs a lot
                continue
            if "\u" not in word_string and "\\x" not in word_string:
                if len(word_string.split(" ")) >=2:
                    word_objects = Keyword.objects.filter(frequency__gt=20,name__exact=word_string).order_by('-frequency')[:2]
                else:
                    word_objects = Keyword.objects.filter(frequency__gt=50,name__exact=word_string).order_by('-frequency')[:2]
                if word_objects and not possibly_sensitive(word_string):
                    word_obj = word_objects[0]
                    generated_themes_dict[log(word_obj.frequency,4)*word[1]] = word_string
        
        #print generated_themes_dict
        theme_range = min(20,len(generated_themes_dict))
        for i in range(0,theme_range):
            max_key = 0
            max_value = ""
            for k,v in generated_themes_dict.iteritems():
                if(k > max_key):
                    max_key = k
                    max_value = v
            del generated_themes_dict[max_key]
            keyword_length = len(max_value)
            #if  keyword_length == 5:
                #if max_value in english_vocab:
                    #i += -1
                    #continue
            flag = 0
            for j in range(0,len(generated_themes_list)):
                word = generated_themes_list[j]
                word_length = len(word)
                if keyword_length-word_length == 1:
                    if max_value[:-1] == word and max_value[-1:] == 's':
                        flag = 1
                        break
                elif word_length-keyword_length == 1:
                    if word[:-1] == max_value and word[-1:] == 's':
                        generated_themes_list[j] = max_value
                        flag = 1
                        break
                elif keyword_length-word_length == 2:
                    if max_value[:-2] == word and max_value[-2:] == 'es':
                        flag = 1
                        break
                elif word_length-keyword_length == 2:
                    if word[:-2] == max_value and word[-2:] == 'es':
                        generated_themes_list[j] = max_value
                        flag = 1
                        break
            if flag != 1:
                generated_themes_list.append(max_value)
            #print max_value, max_key
            if len(generated_themes_dict) == 0:
                break
        
        seen = set()
        seen_add = seen.add
        generated_themes_list = [ x for x in generated_themes_list if not (x in seen or seen_add(x))]
        
        cat_list, individual_keyword_cat_dict = categorize_keywords_individual(generated_themes_list,4)
        if 15 in cat_list:
            cat_list.remove(15)
        if 27 in cat_list:
            cat_list.remove(27)
        cat_list = cat_list[:3]

        filtered_themes_list = filter_unrelated_keywords(generated_themes_list, cat_list, individual_keyword_cat_dict)
        if related_keywords == True:
            return filtered_themes_list + get_related_keywords_from_list(filtered_themes_list, cat_list)
        else:
            return filtered_themes_list
        #print filtered_themes_list
        
def filter_unrelated_keywords(keyword_list, cat_list, individual_keyword_cat_dict):
    filtered_themes_list = []
    for keyword in keyword_list:
        if keyword in individual_keyword_cat_dict:
            temp_cat_tuple = individual_keyword_cat_dict[keyword]
        else:
            temp_cat_tuple = category_scores([keyword])
        temp_cat_list = []
        for (category_index,category_score) in temp_cat_tuple:
            if category_score >= 0.3 and category_index != 15 and category_index != 27:
                temp_cat_list.append(category_index)
        temp_cat_list = temp_cat_list[:3]
        common_categories = list(set(cat_list).intersection(temp_cat_list))
        if len(common_categories):
            filtered_themes_list.append(keyword)
    return filtered_themes_list

def get_related_keywords_from_list(keyword_list, cat_list=[],min_keywords=3):
    final_list = []
    parsed_list = []
    if cat_list == []:
        cat_list = categorize_keywords(keyword_list,4)
        if 15 in cat_list:
            cat_list.remove(15)
        if 27 in cat_list:
            cat_list.remove(27)
        cat_list = cat_list[:3]
    for keyword in keyword_list:
        related_keywords = []
        related_keywords = get_key_cluster_broad(keyword,parsed_list,min_keywords)
        for rel_word in related_keywords:
            if len(rel_word) <= 5 or len(rel_word.split(" ")) >=4 or rel_word.isdigit() or (not is_not_in_stoplist(rel_word)):
                continue
            flag = 0
            for char in rel_word:
                if char.isdigit():
                    i += 1
                    if i >= 2:
                        flag = 1
                        break
            if flag == 1:
                continue
            if possibly_sensitive(rel_word):
                continue
            if rel_word in final_list:
                continue
            if not word_variation_not_in(rel_word,final_list):
                continue
            temp_cat_tuple = category_scores([rel_word])
            temp_cat_list = []
            for (category_index,category_score) in temp_cat_tuple:
                if category_score >= 0.3 and category_index != 15 and category_index != 27:
                    temp_cat_list.append(category_index)
            temp_cat_list = temp_cat_list[:3]
            common_categories = list(set(cat_list).intersection(temp_cat_list))
            if len(common_categories):
                final_list.append(rel_word)
                    #print rel_word + " was added(RELATIVE). Common categories : ", common_categories
            #else:
                #print rel_word + " was removed(RELATIVE). Classified as : ", temp_cat_list
    seen = set()
    seen_add = seen.add
    final_list = [ x for x in final_list if not (x in seen or seen_add(x))]
    return final_list

def learn_from_linkedin_timeline(account):
    if account.usersocialauth.provider not in ['linkedin', 'linkedin-oauth2'] or account.status != 'enabled' or account.account_type != "page":
        return []
    description = ""
    company_updates = get_linkedin_timeline(account)
    if company_updates != False:
        for update in company_updates['values']:
            if 'updateContent' in update:
                if 'comment' in update['updateContent']['companyStatusUpdate']['share']:
                    description += update['updateContent']['companyStatusUpdate']['share']['comment']
                if 'content' in update['updateContent']['companyStatusUpdate']['share']:
                    content = update['updateContent']['companyStatusUpdate']['share']['content']
                    if 'title' in content:
                        if content['title'] not in description:
                            description += content['title']
                    if 'description' in content:
                        if content['description'] not in description:
                            description += content['description']
    if description == "":
        return []
    rake_object = Rake("/home/**********/libs/rake/SmartStoplistv2.txt",3,15,2)  
    kw_list = rake_object.run(description)
    old_rake_object = OldRake("/home/**********/libs/rake/SmartStoplistv2.txt",3,15,2)  
    old_kw_list = old_rake_object.run(description)
    keywords = []
    generated_themes_list = []
    keyword_range = min(30,len(kw_list))
    i = 0
    for kw in kw_list:
        if not is_not_in_stoplist(kw[0],'linkedin'):
            continue
        if kw[1] < 3 and i > 15:
            break
        word_objects = Keyword.objects.filter(frequency__gt=2,name__exact=kw[0])[:5]
        if word_objects and not possibly_sensitive(kw[0]):
            keywords.append(kw)
        i += 1
        if(i>keyword_range):
            break
    for kw in old_kw_list:
        if len(kw[0].split(" ")) >= 2:
            if not is_not_in_stoplist(kw[0],'linkedin'):
                continue
            word_objects = Keyword.objects.filter(frequency__gt=2,name__exact=kw[0])[:5]
            if word_objects and not possibly_sensitive(kw[0]):
                keywords.append((kw[0],5*kw[1]))
    return keyword_filter(keywords,'linkedin')
    

def learn_from_facebook_timeline(account):
    if account.usersocialauth.provider != 'facebook' or account.status != 'enabled' or account.account_type != "page":
        return []
    page_info, page_posts, page_statuses = get_fb_info_posts_statuses(account)
    if page_posts == False or page_statuses == False or page_info == False:
        return []
    #print page_info
    user_info_description = ""
    if 'description' in page_info:
        user_info_description += page_info['description'] +" . "
    if 'about' in page_info:
        user_info_description += page_info['about'] + " . "
    if 'personal_info' in page_info:
        user_info_description += page_info['personal_info'] + " . "
    if 'general_info' in page_info:
        user_info_description += page_info['general_info'] + " . "
    if 'company_overview' in page_info:
        user_info_description += page_info['company_overview'] + " . "
    if 'bio' in page_info:
        user_info_description += page_info['bio'] + " . "
    if 'mission' in page_info:
        user_info_description += page_info['mission'] + " . "
    if 'category_list' in page_info:
        for cat in page_info['category_list']:
            user_info_description += (cat['name'] + " . ")*5
    if 'name' in page_info:
        user_info_description += (page_info['name'] + " . ")*2
    if 'category' in page_info:
        for cat in page_info['category'].split("/"):
            user_info_description += (cat + " . ")*5
    rake_object = Rake("/home/***********/libs/rake/SmartStoplistv2.txt",3,15,2)  
    kw_list = rake_object.run(user_info_description)
    old_rake_object = OldRake("/home/**********/libs/rake/SmartStoplistv2.txt",3,15,2)  
    old_kw_list = old_rake_object.run(user_info_description)
    temp_keywords = []
    keywords = []
    generated_themes_list = []
    keyword_range = min(30,len(kw_list))
    i = 0
    for kw in kw_list:
        if not is_not_in_stoplist(kw[0],'facebook'):
            continue
        if kw[1] < 3 and i > 15:
            break
        temp_keywords.append((kw[0],kw[1]))
        i += 1
        if(i>keyword_range):
            break
    for kw in old_kw_list:
        if len(kw[0].split(" ")) >= 2:
            if not is_not_in_stoplist(kw[0],'facebook'):
                continue
            else:
                temp_keywords.append((kw[0],4*kw[1]))
    for kw in temp_keywords:
        if len(kw[0].split(" ")) in [2,3] and re.match("^[a-zA-Z0-9\s]*$", kw[0]):
            flag = 0
            for j in range(0,len(generated_themes_list)):
                word = generated_themes_list[j]
                word_length = len(word)
                keyword_length = len(kw[0])
                if keyword_length-word_length == 1:
                    if kw[0][:-1] == word and kw[0][-1:] == 's':
                        flag = 1
                        break
                elif word_length-keyword_length == 1:
                    if word[:-1] == kw[0] and word[-1:] == 's':
                        generated_themes_list[j] = kw[0]
                        flag = 1
                        break
                elif keyword_length-word_length == 2:
                    if kw[0][:-2] == word and kw[0][-2:] == 'es':
                        flag = 1
                        break
                elif word_length-keyword_length == 2:
                    if word[:-2] == kw[0] and word[-2:] == 'es':
                        generated_themes_list[j] = kw[0]
                        flag = 1
                        break
            if flag != 1:
                word_objects = Keyword.objects.filter(frequency__gt=2,name__exact=kw[0])[:5]
                if word_objects and not possibly_sensitive(kw[0]):
                    generated_themes_list.append(kw[0])
        else:
            keywords.append(kw)
    #print keywords
    user_post_description = ""
    count = 0
    for post in page_posts['data']:
        if 'message' not in post and 'name' not in post:
            continue
        if 'message' not in post:
            user_post_description += post['name']+" . "
        elif 'name' not in post:
            user_post_description += post['message']+" . "
        else:
            count += 1
            if count > 20:
                break
            if post['message'] == post['name']:
                user_post_description += post['message']+" . "
            else:
                user_post_description += post['message']+" . "+post['name']+" . "
    count = 0
    for status in page_statuses['data']:
        if 'message' not in status:
            continue
        if status['message'] not in user_post_description:
            count += 1
            if count > 20:
                break
            user_post_description += status['message']+" . "
    kw_list = rake_object.run(user_post_description)  
    old_kw_list = old_rake_object.run(user_post_description)
    keyword_range = min(20,len(kw_list))
    i = 0
    for kw in kw_list:
        if not is_not_in_stoplist(kw[0],'facebook'):
            continue
        if kw[1] < 3 and i > 15:
            break
        keywords.append((kw[0],5*kw[1]))
        i += 1
        if(i>keyword_range):
            break
    for kw in old_kw_list:
        if len(kw[0].split(" ")) >= 2:
            if not is_not_in_stoplist(kw[0],'facebook'):
                continue
            keywords.append((kw[0],10*kw[1]))
    keywords = sorted(keywords,key=lambda x: x[1],reverse=True)
    generated_themes_list = generated_themes_list + keyword_filter(keywords,'facebook')
    seen = set()
    seen_add = seen.add
    generated_themes_list = [ x for x in generated_themes_list if not (x in seen or seen_add(x))]     
    return generated_themes_list

def learn_from_twitter_timeline(account):
    if account.usersocialauth.provider != 'twitter' or account.status != 'enabled':
        return []   
    user_account = get_twitter_account(account.usersocialauth)
    user_description = ""
    if user_account != False:    
        user_description = user_account['description'] + " . "
    home_timeline, user_timeline = get_detailed_timeline(account,100,50)
    #start = datetime.utcnow()
    if user_timeline == False and home_timeline == False:
        return []
    #middle = datetime.utcnow()
    tweets_user_list = []
    home_tweets_description = u""
    user_tweets_description = u""
    if home_timeline != False:
        home_timeline = retweet_language_filter(home_timeline,account) #Cannot be applied For user_timeline 
        for tweet in home_timeline:
            if "\u" not in tweet['user']['name'] and "\\x" not in tweet['user']['name'] and tweet['user']['verified'] == True:
                tweets_user_list.append(tweet['user']['name'])
            if tweet['user']['description'] not in home_tweets_description:
                home_tweets_description = home_tweets_description + " " + tweet['user']['description'] + " " #+ tweet['text']
    if user_timeline != False:
        for tweet in user_timeline:
            if tweet['text'] not in user_tweets_description:
                user_tweets_description = user_tweets_description + " " + tweet['text']
    #print user_tweets_description
    tweets_user_list = list(set(tweets_user_list))
    #print tweets_user_list
    # For The Tweets in Home Timeline
    rake_object = Rake("/home/**********/libs/rake/SmartStoplistv2.txt",3,15,2)  
    kw_list = rake_object.run(home_tweets_description)
    if user_description != "":
        home_tweets_description += user_description*10
    old_rake_object = OldRake("/home/**********/libs/rake/SmartStoplistv2.txt",3,15,2)  
    old_kw_list = old_rake_object.run(home_tweets_description)
    keywords = []
    keyword_range = min(20,len(kw_list))
    i = 0
    for kw in kw_list:
        if not is_not_in_stoplist(kw[0],'twitter'):
            continue
        if kw[1] < 3 and i > 10:
            break
        keywords.append(kw)
        i += 1
        if(i>keyword_range):
            break
    for kw in old_kw_list:
        if not is_not_in_stoplist(kw[0],'twitter'):
            continue
        if len(kw[0].split(" ")) >= 2:
            keywords.append((kw[0],5*kw[1]*kw[1]))
        else:
            keywords.append((kw[0],3*kw[1]))
    # For The Tweets in User Timeline
    #print "Home - New ",kw_list
    #print "Home - Old ",old_kw_list
    kw_list = rake_object.run(user_tweets_description)
    if user_description != "":
        home_tweets_description += user_description*10
    old_kw_list = old_rake_object.run(user_tweets_description)
    #print "User - New ",kw_list
    #print "User - Old ",old_kw_list
    keyword_range = min(20,len(kw_list))
    i = 0
    for kw in kw_list:
        if not is_not_in_stoplist(kw[0],'twitter'):
            continue
        if kw[1] < 3 and i > 10:
            break
        keywords.append((kw[0],5*kw[1]))
        i += 1
        if(i>keyword_range):
            break
    for kw in old_kw_list:
        if not is_not_in_stoplist(kw[0],'twitter'):
            continue
        if len(kw[0].split(" ")) >= 2:
            keywords.append((kw[0],15*kw[1]*kw[1]))
        else:
            keywords.append((kw[0],5*kw[1]))
    keywords = sorted(keywords,key=lambda x: x[1],reverse=True)
    #print keywords
    if keywords:
        max_keyword_value = keywords[0][1]
    #middle1 = datetime.utcnow()
    generated_themes_list = keyword_filter(keywords,'twitter')
    #middle2 = datetime.utcnow()


    seen = set()
    seen_add = seen.add
    generated_themes_list = [ x for x in generated_themes_list if not (x in seen or seen_add(x))]



    #For the twitter names of verified Profiles
    # Change the number fr the if statement below to a finite value to enable displaying names of verified profiles
    
    if len(generated_themes_list) >= -1:
        return generated_themes_list[:20]
    user_keywords = []
    for word in tweets_user_list:
        if len(word) <= 2:
            continue
        word = word.lower()
        word_objects = Keyword.objects.filter(frequency__gt=2,name__exact=word).order_by('-frequency')[:5]
        if word_objects:
            word_obj = word_objects[0]
            if word.split(" ") >=2:
                user_keywords.append((word,word_obj.frequency*max_keyword_value))
            else:
                user_keywords.append((word,word_obj.frequency))
    user_keywords = sorted(user_keywords,key=lambda x: x[1],reverse=True)[:10]
    #print user_keywords
    for user_obj in user_keywords:
        generated_themes_list.append(user_obj[0])
    seen = set()
    seen_add = seen.add
    generated_themes_list = [ x for x in generated_themes_list if not (x in seen or seen_add(x))]
    #end = datetime.utcnow()
    #print "Processing time: ", (end - start).total_seconds()
    #print "Keyword Filter time: ", (middle2 - middle1).total_seconds()
    return generated_themes_list[:20]#, (end - start).total_seconds()
    #print "Twitter Time :", (middle-start).total_seconds()
    #print "Total Time: ", (end - start).total_seconds()


def get_keywords_from_text(text):
    text = sanitize(text)
    rake_object = Rake("/home/**********/libs/rake/SmartStoplistv2.txt",3,15,2)  
    rake_keywords = rake_object.run(text)
    filtered_keywords = keyword_filter(rake_keywords,'all', False)
    return filtered_keywords
