from __future__ import division
import re
from socialstats.apps.socialposter.models import Category,KeywordCategory,Keyword,Story,PostingAccounts,StoryArchive
from socialstats.apps.socialposter.common import get_keywords_from_tags
from socialstats.libs.rake.rakev2 import Rake, separate_words
from collections import defaultdict
from socialstats.settings import SITE_ROOT

def get_cat_freq():
    for cat in Category.objects.all():
        freq = 0
        """
        for s in Story.objects.filter(categorization_status__in=[1,2]):
            if cat in s.category.all():
                freq += 1
        """
        freq = Story.objects.filter(categorization_status__in=[1,2]).filter(category=cat).count()
        acount = StoryArchive.objects.filter(category=cat).count()
        #print cat.expression
        #print freq
        freq += acount
        #print freq
        cat.frequency = freq
        cat.save()

def categorize_archive():
    for story in StoryArchive.objects.filter(categorization_status=0).order_by('id')[:50000]:
        if story.categorization_status > 0:
            return
        text = (story.url + story.tags).lower()
        for category in Category.objects.all():
            if len(re.findall(category.expression,text)):
                story.category.add(category)
        if len(story.category.all()):
            story.categorization_status = 1
        else:
            story.categorization_status = 3
        story.save()


def test4():
    for story in Story.objects.exclude(categorization_status=0).order_by('-id')[:1]:
        print 'url: ' + story.url
        print 'tags:' + story.tags
        print 'status: ' + str(story.categorization_status)
        for cat in story.category.all():
            print cat.expression

def categorize(story):
    if story.categorization_status > 0:
        return
    text = (story.url + story.tags).lower()
    for category in Category.objects.all():
        if len(re.findall(category.expression,text)):
            story.category.add(category)
    if story.category.exists():
        learn_from_categorization(story)
    else:
        categorize_uncategorized(story)
    
def learn_from_categorization(story):
    keywords = get_keywords_from_tags(story.tags)[:15]
    if not keywords:
        story.categorization_status = 1
        story.save()
        return
    for category in story.category.all():
        category.frequency += 1
        category.save()
        for kw in keywords:
            kwobj, created = Keyword.objects.get_or_create(name=kw)
            pair, created = KeywordCategory.objects.get_or_create(keyword=kwobj, category=category)
            pair.strength += 1
            pair.save()
    story.categorization_status = 2
    story.save()

def get_keywords(story):
    keywords = get_keywords_from_tags(story.tags)
    predicted_tags = get_keywords_from_rake(story)
    if not keywords:
        keywords = predicted_tags 
    #story.predicted_tags = (',').join(predicted_tags)
    story.categorization_status = 3
    story.save()
    return keywords

def get_keywords_from_rake(story):
    #print "I AM IN RAKE"
    #rake_object = Rake("/home/production/venv/socialstats/socialstats/libs/rake/SmartStoplistv2.txt",3,15,2)	
    rake_object = Rake(SITE_ROOT + "/libs/rake/SmartStoplistv2.txt",3,15,2)	
    kw_list = rake_object.run(story.title+"\n\n"+story.teaser2)
    keywords = []
    for kw in kw_list:
        keywords.append(kw[0])
    #print keywords
    return keywords[:10]

def categorize_uncategorized(story):
    story.categorization_status = 3
    categories = []
    #print "A"
    keywords = get_keywords_from_tags(story.tags)[:15]
    if not keywords:
        categories=categorize_url(story.url)
    #print "url"
    if not keywords:
        keywords = get_keywords_from_rake(story) 
        story.categorization_status = 4
    if not keywords and (not categories):
        story.categorization_status = 5
        cat_other = Category.objects.get(expression='xx_other')
        story.category.add(cat_other)
    else:
        if not categories:
        #print "not url"
            categories = categorize_keywords(keywords)
        else:
            story.categorization_status = 3
    for category in categories:
            story.category.add(category)
    story.save()

def categorize_keywords(keywords,min=1):
    key_count = 0
    cat_other = Category.objects.get(expression='xx_other')
    #categories = {cat_other.id:0}
    categories = defaultdict(float)
    original_len=len(keywords)
    for kw in keywords:
        qset = KeywordCategory.objects.filter(keyword__name=kw)
        sum=0
        cat_key=defaultdict(int)
        if not qset:
            if isinstance(kw,basestring):
                if kw.find(' ')!=-1:
                    for wo in separate_words(kw,0):
                        keywords.append(wo)
                else :
                    categories[cat_other.id] +=0.2
                    key_count += 1
            continue
        key_count += 1
        for obj in qset:
            cat_key[obj.category.id] += obj.strength   
            sum += obj.strength
        for num,value in cat_key.iteritems():
            categories[num] +=value/sum
	    
    cat_list = sorted(categories.items(), key=lambda l:l[1],reverse=True)
    #print cat_list
    cats = []
    # we choose all categories that are within 0.4 of the strength of the top cat
    if len(cat_list):
        sum = 0
        for cat in cat_list:
            sum += cat[1]
        avg = sum/len(cat_list)
    count = 0
    for cat in cat_list:
        if cat[1] > max(0.1*key_count, 0.4*cat_list[0][1]):
            cats.append(cat[0])
            #print cat[0]
            count+=1
        elif count<min:
            if count<(min-1):
                cats.append(cat[0])
                count += 1
        """
        if cat[1] > max(5, avg):
            categories.append(cat[0])
        """
    if len(cats)<min:
	#print "other2"
        cats.append(cat_other.id)
    """
    if not len(cat_list):
        categories.append(cat_other.id)
    else:
        categories.append(cat_list[0][0])
        if len(cat_list) >1:
            categories.append(cat_list[1][0])
    """
    #print key_count
    #print cats
    return cats

def url_words(url):
    
    splitter = re.compile('[^a-zA-Z]')
    words = []
    ignore_words=['http','https','www','com','org','net','biz','info','mobi','html']
    for single_word in splitter.split(url):
        current_word = single_word.strip().lower()
        if (len(current_word) > 2) and (not (current_word in ignore_words)):
            words.append(current_word)
    url_text=(" ").join(words)
    #rake_object = Rake("/home/production/venv/socialstats/socialstats/libs/rake/SmartStoplistv2.txt",3,15,1)
    rake_object = Rake(SITE_ROOT + "/libs/rake/SmartStoplistv2.txt",3,15,1)
    kw_list = rake_object.run(url_text)
    keywords = []
    for kw in kw_list:
            keywords.append(kw[0])
    return keywords


def categorize_url(url,min=0):
    key_count = 0
    keywords=url_words(url)
    cat_other = Category.objects.get(expression='xx_other')
    #categories = {cat_other.id:0}
    categories = defaultdict(float)
    for kw in keywords:
        qset = KeywordCategory.objects.filter(keyword__name=kw)
        sum=0
        cat_key=defaultdict(int)
        #implementation of naive bayes method
        categorized =0
        for obj in qset:
            cat_key[obj.category.id] += obj.strength
            sum += obj.strength
        for num,value in cat_key.iteritems():
            if((value/sum)>0.5) and sum>5:
                categories[num] +=value/sum
                categorized =1
		#print kw
	if categorized:
	    key_count += 1

    cat_list = sorted(categories.items(), key=lambda l:l[1],reverse=True)
    #print cat_list

    cats = []
    # we choose all categories that are within 0.4 of the strength of the top cat
    if len(cat_list):
        sum = 0
        for cat in cat_list:
            sum += cat[1]
        avg = sum/len(cat_list)
    count = 0
    for cat in cat_list:
        if cat[1] > max(0.1*key_count, 0.4*cat_list[0][1]):
            cats.append(cat[0])
            count+=1
        elif count<min:
            if count<(min-1):
                cats.append(cat[0])
                count += 1
        
    if len(cats)<min:
        cats.append(cat_other.id)
    
    return cats


def categorize_account(account):
    if not account.post_themes:
        return
    account_categories = account.category.all()
    local_cat = Category.objects.get(expression='local')
    other_cat = Category.objects.get(expression='xx_other')
    for cat in account_categories:
        account.category.remove(cat)
    for cat in categorize_keywords(account.post_themes.split(','),2):
        if cat == local_cat.id:
            cat = other_cat.id
        account.category.add(cat)
    # replace local cat with other
    """
    if local_cat in account_categories:
        account.category.remove(local_cat)
        if other_cat not in account_categories:
            account.category.add(other_cat)
    """

def categories_str(obj):
    cats = ''
    for cat in obj.category.all():
        cats += cat.expression + ', '
    return cats[:-2]

"""
def categorize_all_accounts():
    for p in PostingAccounts.objects.all():
        #print p.id
"""

####### The below functions are for Keyword Suggestion ##########

def categorize_keywords_individual(keywords,min=1):
    key_count = 0
    cat_other = Category.objects.get(expression='xx_other')
    #categories = {cat_other.id:0}
    individual_keyword_cat = {}
    categories = defaultdict(float)
    for kw in keywords:
        qset = KeywordCategory.objects.filter(keyword__name=kw)
        sum=0
        cat_key=defaultdict(int)
        if not qset:
            if isinstance(kw,basestring):
                if kw.find(' ')!=-1:
                    for wo in separate_words(kw,0):
                        keywords.append(wo)
                else :
                    categories[cat_other.id] +=0.2
                    key_count += 1
            continue
        key_count += 1
        for obj in qset:
            cat_key[obj.category.id] += obj.strength   
            sum += obj.strength
        for num,value in cat_key.iteritems():
            categories[num] +=value/sum
        for key in cat_key:
            cat_key[key] = cat_key[key]/sum
        temp_cat_list = sorted(cat_key.items(), key=lambda l:l[1],reverse=True)
        individual_keyword_cat[kw] = temp_cat_list[:5]
        
    cat_list = sorted(categories.items(), key=lambda l:l[1],reverse=True)
    #print cat_list
    cats = []
    # we choose all categories that are within 0.4 of the strength of the top cat
    if len(cat_list):
        sum = 0
        for cat in cat_list:
            sum += cat[1]
        avg = sum/len(cat_list)
    count = 0
    for cat in cat_list:
        if cat[1] > max(0.1*key_count, 0.4*cat_list[0][1]):
            cats.append(cat[0])
            #print cat[0]
            count+=1
        elif count<min:
            if count<(min-1):
                cats.append(cat[0])
                count += 1
        """
        if cat[1] > max(5, avg):
            categories.append(cat[0])
        """
    if len(cats)<min:
    #print "other2"
        cats.append(cat_other.id)
    """
    if not len(cat_list):
        categories.append(cat_other.id)
    else:
        categories.append(cat_list[0][0])
        if len(cat_list) >1:
            categories.append(cat_list[1][0])
    """
    #print key_count
    #print cats
    return cats, individual_keyword_cat


def category_scores(keywords,min=1):
    key_count = 0
    cat_other = Category.objects.get(expression='xx_other')
    #categories = {cat_other.id:0}
    categories = defaultdict(float)
    for kw in keywords:
        qset = KeywordCategory.objects.filter(keyword__name=kw)
        sum=0
        cat_key=defaultdict(int)
        if not qset:
            if isinstance(kw,basestring):
                if kw.find(' ')!=-1:
                    for wo in separate_words(kw,0):
                        keywords.append(wo)
                else :
                    categories[cat_other.id] +=0.2
                    key_count += 1
            continue
        key_count += 1
        for obj in qset:
            #print obj.category.id, obj.strength
            cat_key[obj.category.id] += obj.strength   
            sum += obj.strength
        for num,value in cat_key.iteritems():
            categories[num] +=value/sum
        
    cat_list = sorted(categories.items(), key=lambda l:l[1],reverse=True)
    return cat_list[:5]
