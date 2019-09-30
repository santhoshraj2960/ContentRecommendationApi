from autosocial.apps.socialposter.models import (LibraryPosts, Story, StoryUserFeed, SaveStoryQueue, 
    KeywordAnalysis, Keyword, EAPosts, OriginalUrlUserFeed, UrlCopiesUserFeed, User, ContentStreamRss)
from autosocial.apps.spider.models import UserFeed, Feed
import feedparser
from django.db import IntegrityError
from django.db.models import Q
from social_apis import get_timeline
from url_analysis import (analyze_url, associate_account_with_story, story_authority,
                          save_orig_url, possibly_sensitive,get_website_from_domain,
                          get_original_url_from_db, associate_url_with_account,
                          strip_suffix, analyze_user_feed_url, GetURLinfo2)
from hits import analyse_story
from dateutil import parser
from time import mktime
from autosocial.apps.socialposter.common import get_keywords_from_tags,sanitize,resize_and_crop,sanitize_digest
from autosocial.apps.socialposter.topicweb import build_topicweb
from autosocial.apps.socialposter.category_update import categorize, get_keywords_from_rake 
import logging
from django.utils.timezone import now, make_aware
from datetime import datetime, timedelta
from tld import get_tld
#import django_rq
import pytz
import boto
from boto.s3.key import Key
#from PIL import Image
from django.utils import timezone
from hashtag import find_hashtags
from website_popularity_check import update_website_popularity
import re
from celery import shared_task
from django.core import serializers
from search import es_index_story
import os
import shutil
from celery.exceptions import SoftTimeLimitExceeded
from autosocial.settings import CLOUD_ACCESS_KEY_ID_FOR_IMAGE_UPLOAD, CLOUD_SECRET_ACCESS_KEY_FOR_IMAGE_UPLOAD

CLOUD_ACCESS_KEY_ID = CLOUD_ACCESS_KEY_ID_FOR_IMAGE_UPLOAD
CLOUD_SECRET_ACCESS_KEY = CLOUD_SECRET_ACCESS_KEY_FOR_IMAGE_UPLOAD

#logger = logging.getLogger(__name__)
logger = logging.getLogger('autosocial.spider')

@shared_task()
def new_stories_from_twitter(account):
    account = serializers.deserialize("json", account).next().object
    #resp = usr_twython.verify_credentials()
    resp = get_timeline(account)
    if not resp:
        return
    resp = tweet_elimination(resp,account)
    #q = django_rq.get_queue('save_story_twitter')
    for tweet in resp:
        for urrl in tweet['entities']['urls']:
            if urrl['expanded_url'].startswith('https://twitter.com'): #these are just statuses which does not contain any article link
                continue
            if not story_new_and_valid(None,urrl['expanded_url'],account,None):
                continue
            serialized_account_obj = serializers.serialize('json', [ account, ])
            #save_story.apply_async(args=[urrl['expanded_url'],'twitter', tweet['created_at'], None, serialized_account_obj, None], queue='save_twitter_story')
            #retrieve_url.apply_async(args=[urrl['expanded_url'],'twitter', tweet['created_at'], None, serialized_account_obj, None], queue='network_queue')
            save_story.apply_async(args=[urrl['expanded_url'],'twitter', tweet['created_at'], None, serialized_account_obj, None], queue='save_twitter_story')
            #q.enqueue(save_story,urrl['expanded_url'],'twitter', parser.parse(tweet['created_at']), None, account,None, timeout=60)
            update_website_popularity(urrl['expanded_url'], account, tweet['id_str'])


def story_in_save_story_queue(title,url):
    if SaveStoryQueue.objects.filter(url=url).exists():
        return True
    elif title and (title != '') and SaveStoryQueue.objects.filter(title=title).exists():
        return True
    else:
        if not title:
            title = ''
        SaveStoryQueue.objects.create(url=url, title=title, timestamp=now())
        return False

def story_new_and_valid(title,url,account,commercial):
    if story_in_save_story_queue(title, url):
        return False
    txt = url
    if title: 
        if commercial:
            q = StoryUserFeed.objects.filter(title=title)
        else:
            q = Story.objects.filter(title=title)
        if len(title.split()) < 5 or q.exists():
            return False
        txt += title
    if not commercial and possibly_sensitive(txt):
        return False
    url_in_db = get_original_url_from_db(url)
    if url_in_db:
        if account:
            associate_url_with_account(url_in_db,account)
        return False
    return True

def get_article_website(article_url, channel, feed=None):
    if channel == 'twitter':
        website = get_website_from_domain(get_tld(article_url))
        return website
        """
        if not website or website.status not in ['active','no_feeds_found']:
            logger.debug('Discard - website not in db or not active ' + article.url)
            return False
        """
    else:
        return feed.website

'''
@shared_task
def retrieve_url(url, channel, pub_time, title=None, account=0, feed_id=None, story_update=False):
    if channel == 'commercial_feed':
    article = analyze_user_feed_url(url)
    else:
    if account:
        account_obj = serializers.deserialize("json", account)
        else:
            account_obj = 0
        article = analyze_url(url, account_obj)
    #serialized_article_obj = serializers.serialize('json', [ article, ])
    if not article:
        return
    if len(article.title) >= 200:
        new_title = article.title[:900] + '...'
    else:
        new_title = article.title
    article_dict = {'url': article.url, 'title': new_title, 'meta_description': article.meta_description if article.meta_description else '', 
    'cleaned_text': article.cleaned_text if article.cleaned_text else '', 'top_image_width': article.top_image.width if article.top_image else 0,
    'top_image_height': article.top_image.height if article.top_image else 0, 'meta_keywords': article.meta_keywords if article.meta_keywords else '', 'user_mentions': article.user_mentions,
     'top_image_filename': article.top_image.filename if article.top_image else '' }
    if channel == 'twitter':
        save_story.apply_async(args=[url,'twitter', pub_time, article_dict, None, account], queue='save_twitter_story')
    elif channel == 'feed':
        save_story.apply_async(args=[url,'feed', pub_time, article_dict, title, 0, feed_id], queue='save_story')
    else:
        save_commercial_story.apply_async(args=[url, pub_time, feed_id, False, article_dict], queue='save_commercial_story')
    copy_file_to_required_image_directory(article_dict)
'''

@shared_task
def fetch_image_lib_post(post_id):
    post = LibraryPosts.objects.get(id=post_id)
    text = post.text
    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
    if not urls:
        return
    article = GetURLinfo2(urls[0])
    # image_url = get_s3_image_url(article, 'aso5')
    image_url = get_s3_image_url(article, 'drulib')
    if image_url:
        post.image_url = image_url
        post.save()

@shared_task
def save_story(url, channel, pub_time, title=None, account=0, feed_id=None, story_update=False):
    if feed_id:
        feed = Feed.objects.get(id=feed_id)
        #feed = serializers.deserialize("json", feed).next().object
    else:
        feed = None
    if not account == 0:
        account = serializers.deserialize("json", account).next().object
    try:
        article = analyze_url(url, account)
    except SoftTimeLimitExceeded:
        logger.info('time limit exceeded for the save_story url = ', url, ' feed = ', feed_id)
    if not article:
        return False
    if not title:
        title = article.title
        title = strip_suffix(title, article.domain.split('.')[0])
    if not title:
        return False 
    
    if len(title) > 195:
        title = title[:190] + ' ...'

    #story_exists = Story.objects.filter(Q(title=title) | Q(url=article.url)).first()
    #if feed and story_exists:
    #    story_exists.feeds.add(feed)
    #    story_exists.save()
    #    return False
    #elif story_exists:
    #    return False
    website = get_article_website(article.url, channel, feed)
    """
    if not feed:
        return False
    """
    if not website or website.status != 'active':
        return False
    auth = story_authority(article.url,website,channel)
    if story_update:
        s, created = Story.objects.get_or_create(url=url)
        s.authority = auth
    else:
        s = Story(url=article.url,title=title,published_time=parser.parse(pub_time),authority=auth,channel=channel)
    s.teaser3 = sanitize(title)
    s.teaser = sanitize(article.meta_description[:500].strip()) if article.meta_description else ""
    s.teaser2 = sanitize(article.cleaned_text[:10000]) if article.cleaned_text else ""
    s.description = sanitize_digest(article.meta_description[:299].strip()) if article.meta_description else ""
    try:
        image_url = get_s3_image_url(article)
    except Exception as e:
        logger.info('PIL Exception for url : ' + article.url + 'Exception: ' + str(e))
        image_url = ''
    s.image_url = image_url
    s.image_width = article.top_image.width if article.top_image else 0
    s.image_height = article.top_image.height if article.top_image else 0
    s.website = website
    s.country = s.website.country
    s.feed = feed
    #s.feeds.add(feed)
    s.created_time = timezone.now()
    # TODO - should be using article.tags below?
    s.tags = sanitize(article.meta_keywords[:300],True) if article.meta_keywords else ""
    predicted_tags = get_keywords_from_rake(s)
    s.predicted_tags = (',').join(predicted_tags)
    s.hashtags = find_hashtags(s)
    s.user_mentions = article.user_mentions if len(article.user_mentions) < 500 else ','.join(article.user_mentions[:498].split(',')[:-1])
    #s.user_mentions = article.user_mentions
    #story_exists = Story.objects.filter(Q(title=title) | Q(url=article.url)).first()
    #if story_exists:
    #    if feed:
    #        story_exists.feeds.add(feed)
    #        story_exists.save()
    #        return False
    #    else:
    #        return False
    try:
        s.save()
        if feed:
            s.feeds.add(feed)
        save_orig_url(article.url,s)
        #keyword_populate(s)
    except IntegrityError as e:
        logger.info("Integrity error: " + str(e) + " channel = " + channel + '  article_url = '+ article.url + ' url= ' + url) 
        if feed:
            check_integrity_error(feed, article.url)
        return False
    except Exception as e:
        logger.error("Could not save story: " + str(article.url) + ' article: ' + str(article) + " Error: "+ str(e))
        return False
    if channel == 'twitter':
        associate_account_with_story(account,s)
    else:
        feed.items_fetched += 1
        feed.save()
    categorize(s)
    analyse_story(s)
    if len(s.tags) > 1:
        build_topicweb(get_keywords_from_tags(s.tags)[:15])
    es_index_story(s, article)
        

@shared_task
def save_story_old(url, channel, pub_time, title=None, account=0, feed_id=None, story_update=False):
    #article = serializers.deserialize("json", article).next().object
    if feed_id:
        feed = Feed.objects.get(id=feed_id)
        #feed = serializers.deserialize("json", feed).next().object
    else:
        feed = None
    if not account == 0:
        account = serializers.deserialize("json", account).next().object
    article = analyze_url(url, account)
    if not article:
        return False
    website = get_article_website(article['url'], channel, feed) # ______-------->
    """
    if not feed:
        return False
    """
    if not website or website.status != 'active':
        return False
    auth = story_authority(article['url'],website,channel)
    if story_update:
        s, created = Story.objects.get_or_create(url=url)
        s.authority = auth
    else:
        #s = Story(url=article.url,title=article.title,published_time=parser.parse(pub_time),authority=auth,channel=channel)
        s = Story(url=article['url'],title=article['title'],published_time=parser.parse(pub_time),authority=auth,channel=channel)
    s.teaser3 = sanitize(article['title'])
    s.teaser = sanitize(article['meta_description'][:500].strip())
    s.teaser2 = sanitize(article['cleaned_text'][:10000])
    s.description = sanitize_digest(article['meta_description'][:299].strip())
    s.image_url = get_s3_image_url(article)
    s.image_width = article['top_image_width']
    s.image_height = article['top_image_height']
    s.website = website
    s.country = s.website.country
    s.feed = feed
    #s.feeds.add(feed)
    s.created_time = timezone.now()
    # TODO - should be using article.tags below?
    s.tags = sanitize(article['meta_keywords'][:300],True)
    predicted_tags = get_keywords_from_rake(s)
    s.predicted_tags = (',').join(predicted_tags)
    s.hashtags = find_hashtags(s)
    s.user_mentions = article['user_mentions']
    try:
        s.save()
        if feed:
            s.feeds.add(feed)
        save_orig_url(article['url'],s)
        #keyword_populate(s)
    except IntegrityError as e:
        logger.error("Integrity error: " + str(e))
        if feed:
            check_integrity_error(feed, article['url'])
        return False
    #except Exception as e:
    #    logger.error("Could not save story: " + str(article['url']) + ' article: ' + str(article) + " Error: "+ str(e))
    #    return False
    if channel == 'twitter':
        associate_account_with_story(account,s)
    else:
        feed.items_fetched += 1
        feed.save()
    categorize(s)
    analyse_story(s)
    if len(s.tags) > 1:
        build_topicweb(get_keywords_from_tags(s.tags)[:15])
    es_index_story(s, article)
    copy_file_to_required_image_directory(article)

    '''
    if article['top_image_filename']:
        if os.path.exists(article['top_image_filename'].split('.')[0]):
            os.remove(article['top_image_filename'].split('.')[0])

        if os.path.exists('/tmp/goose/s' + article['top_image_filename'].split('/')[3]):
            os.remove('/tmp/goose/s' + article['top_image_filename'].split('/')[3])
    '''

def copy_file_to_required_image_directory(article):
    if not os.path.exists('/tmp/goose/required_img'):
        os.makedirs('/tmp/goose/required_img')
    if os.path.exists(article['top_image_filename'].split('.')[0]):
        try:
            shutil.move(article['top_image_filename'].split('.')[0] , '/tmp/goose/required_img/' + article['top_image_filename'].split('/')[3].split('.')[0])
        except IOError: #due to parallel processing the file has been already moved by a different task
            pass


def keyword_populate(story):
    # s = Story.objects.all().order_by('id')
    # for story in s:
    if(story.tags):
        keys = story.tags
        if(',' in keys):
            keys = keys.replace(', ',',')
            keys = keys.split(',')
            for key in keys:
                kw, create = Keyword.objects.get_or_create(name=key)
                obj, created = KeywordAnalysis.objects.get_or_create(kw=kw)
                if obj.body_occurrences == None:
                    obj.body_occurrences = 0
                if obj.analyze_count == None:
                    obj.analyze_count = 0
                obj.body_occurrences += len(re.findall(key,story.teaser2))
                obj.analyze_count += 1
                obj.ratio = float(obj.body_occurrences) / float(obj.analyze_count)
                obj.save()


@shared_task
def save_commercial_story_old(url, pub_time, feed_id, story_update=False, article=None):
    feed = UserFeed.objects.get(id=feed_id)
    #feed = serializers.deserialize("json", feed).next().object
    #article = analyze_url(url, account=0)
    #article = analyze_user_feed_url(url)
    if not article:
        return False
    if not feed:
        return False
    if story_update:
        s, created = StoryUserFeed.objects.get_or_create(url=url)
        comp_title = s.title # for company post update
        comp_url = s.url     # for company post update
        s.url = article['url']
        s.title = article['title']
    else:
        #s = StoryUserFeed(url=article.url,title=article.title,published_time=parser.parse(pub_time),feed=feed)
        s = StoryUserFeed(url=article['url'],title=article['title'],title_lower=article['title'].lower(),published_time=parser.parse(pub_time))
    #s.feeds.add(feed)
    image_url = get_s3_image_url(article)
    s.image_url = image_url
    if story_update:
        copy_image_to_company_posts(comp_title, comp_url, image_url)
    s.image_width = article['top_image_width']
    s.image_height = article['top_image_height']
    s.teaser2 = sanitize(article['cleaned_text'][:10000])
    s.tags = sanitize(article['meta_keywords'][:300],True)
    predicted_tags = get_keywords_from_rake(s)
    s.predicted_tags = (',').join(predicted_tags)
    s.hashtags = find_hashtags(s)
    s.user_mentions = article['user_mentions']
    # once keyword prediction is done, we don't need body any longer, just save description
    s.teaser2 = article['meta_description'][:500].strip()
    try:
        s.save()
        s.user_feed.add(feed)
    except IntegrityError as e:
        logger.error("Integirty error: " + str(e))
        check_integrity_error_user_feed(feed, article['url'])
        return False
    '''except Exception as e:
        logger.error("Could not save commercial story: " + str(article.url) + ' article: ' + str(article) + " Error: "+ str(e))
        return False
    '''
    feed.items_fetched += 1
    feed.save()
    copy_file_to_required_image_directory(article)


@shared_task
def save_commercial_story(url, pub_time, feed_id, story_update=False, article=None, title=None):
    feed = UserFeed.objects.get(id=feed_id)
    #feed = serializers.deserialize("json", feed).next().object
    #article = analyze_url(url, account=0)
    try:
        article = analyze_user_feed_url(url)
    except SoftTimeLimitExceeded:
        logger.info('time limit exceeded for the save_commercial_story feed url = ', url, ' feed = ', feed_id)
    if not article:
        return False
    original_url = OriginalUrlUserFeed.objects.get_or_create(url=article.url)
    created = original_url[1]
    original_url = original_url[0]
    if not url.strip('/') == article.url:
        url_copies = UrlCopiesUserFeed.objects.get_or_create(url=url.strip('/'),original_url=original_url)
    if not created:
        story_user_feed = original_url.story_user_feed
        if story_user_feed:
            story_user_feed.user_feed.add(feed)
            story_user_feed.save()
            return False
    if not title:
        title = article.title
        title = strip_suffix(title,article.domain.split('.')[0])
    if not title:
        return False
    if len(title) > 195:
        title = title[:190] + ' ...'
    if not feed:
        return False
    if story_update:
        s, created = StoryUserFeed.objects.get_or_create(url=url)
        comp_title = s.title # for company post update
        comp_url = s.url     # for company post update
        s.url = article.url
        s.title = article.title   
    else:
        s = StoryUserFeed(url=article.url,title=title,title_lower=title.lower(),published_time=parser.parse(pub_time))
    #s.feeds.add(feed)
    try:
        image_url = get_s3_image_url(article)
    except Exception as e:
        logger.info('PIL Exception for url : ' + article.url + 'Exception: ' + str(e))
        image_url = ''
    s.image_url = image_url
    if story_update:
        copy_image_to_company_posts(comp_title, comp_url, image_url)
    s.image_width = article.top_image.width if article.top_image else 0
    s.image_height = article.top_image.height if article.top_image else 0
    s.teaser2 = sanitize(article.cleaned_text[:10000]) if article.cleaned_text else ""
    s.tags = sanitize(article.meta_keywords[:300],True) if article.meta_keywords else ""
    predicted_tags = get_keywords_from_rake(s)
    s.predicted_tags = (',').join(predicted_tags)
    s.hashtags = find_hashtags(s)
    s.user_mentions = article.user_mentions if len(article.user_mentions) < 500 else ','.join(article.user_mentions[:498].split(',')[:-1])
    #s.user_mentions = article.user_mentions
    #if create_manytomany_relation_user_feed_table(feed, title, article.url):
    #    return False
    try:
        s.save()
        s.user_feed.add(feed)
    except IntegrityError as e:
        logger.info("Integirty error: " + str(e))
        check_integrity_error_user_feed(feed, article.url)
        story_user_feed = StoryUserFeed.objects.filter(url=article.url).first()
        if s:
            original_url.story_user_feed = story_user_feed
            original_url.save()
        feed.items_fetched += 1
        feed.save()
        return False
    except Exception as e:
        logger.error("Could not save commercial story: " + str(article.url) + ' article: ' + str(article) + " Error: "+ str(e)+' tags = '+s.tags+' predicted_tags= '+s.predicted_tags+' hashtags='+s.hashtags+' um = '+s.user_mentions )
        return False
    
    original_url.story_user_feed = s
    original_url.save()
    feed.items_fetched += 1
    feed.save()


def get_s3_image_url_old(article):
    if  len(article['top_image_filename']) > 4 and '.' in article['top_image_filename']:
        BUCKET_NAME='aso2'
        conn= boto.connect_s3(CLOUD_ACCESS_KEY_ID,CLOUD_SECRET_ACCESS_KEY)
        bucket= conn.get_bucket(BUCKET_NAME)
        k=Key(bucket)
        filename = article['top_image_filename']
        k.key=filename.split('/')[3]
        if os.path.exists(article['top_image_filename'].split('.')[0]):
            name_file= article['top_image_filename'].split(".")[0]
            k.set_contents_from_filename(name_file)
        elif os.path.exists('/tmp/goose/required_img/' + article['top_image_filename'].split('/')[3].split('.')[0]):
            name_file = '/tmp/goose/required_img/' + article['top_image_filename'].split('/')[3].split('.')[0]
            k.set_contents_from_filename(name_file)
        else:
            return ''
        k.make_public()
        get_s3_thumbnail_url(bucket, k.key, name_file)
        return 'https://s3.amazonaws.com/aso2/' + k.key
    else:
        return ""

@shared_task
def get_image_from_urls(post_id_url_dict):
    for post_id in post_id_url_dict:
        post_text = post_id_url_dict[post_id]
        regex_url = re.search("(?P<url>https?://[^\s]+)", post_text)
        if regex_url: 
            url = regex_url.group("url")
            article = GetURLinfo2(url)
            if not article:
                return
            # image_url = get_s3_image_url(article, 'aso5')
            image_url = get_s3_image_url(article, 'drulib')
            lib_post = LibraryPosts.objects.get(id=post_id)
            lib_post.image_url = image_url
            lib_post.save()

# get the S3 image url
# def get_s3_image_url(article, BUCKET_NAME='aso2'):
def get_s3_image_url(article, BUCKET_NAME='drustory'):
    if article.top_image and len(article.top_image.filename) > 4 and '.' in article.top_image.filename:
        conn= boto.connect_s3(CLOUD_ACCESS_KEY_ID,CLOUD_SECRET_ACCESS_KEY)
        bucket= conn.get_bucket(BUCKET_NAME)
        k=Key(bucket)
        filename =article.top_image.filename
        k.key=filename.split('/')[3]
        name_file=article.top_image.filename.split(".")
        k.set_contents_from_filename(name_file[0])
        k.make_public()
        get_s3_thumbnail_url(bucket, k.key, filename)
        return 'https://s3.amazonaws.com/' + BUCKET_NAME + '/' + k.key
    else:
        return ""


def get_s3_thumbnail_url(bucket, key, filename):
    infile = filename.split('.')[0]
    size = (125, 100)
    outfile = '/tmp/goose/s'+key
    r = Key(bucket)
    """
    im = Image.open(infile)
    im.thumbnail(size)
    im.save(outfile)
    """
    #if not os.path.exists(infile):
    #    infile = '/tmp/goose/required_img/' + infile.split('/')[3]
    resize_and_crop(infile, outfile, size, 'middle')
    r.key = 's'+key
    r.set_contents_from_filename(outfile)
    r.make_public()


# to copy image url to company posts added from immediate fetch of blog feed
def copy_image_to_company_posts(title, url, image_url):
    text = title.strip() + ' ' + url.strip()
    company_post = EAPosts.objects.filter(text=text)
    for post in company_post:
        post.image_url = image_url
        post.save()
 

# elimiate tweets that have no value - e.g. no url, wrong language etc.
def tweet_elimination(resp,account):
    # b=[]
    resp2 = resp
    # last_time=datetime.strptime(resp2[0]['created_at'],'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=timezone.utc)
    #last_time = parser.parse(resp2[0]['created_at'])
    i = 0
    while i < len(resp2):
        #if parser.parse(resp2[i]['created_at']) < user.usersettings.updated_storyfetch:
        time_threshold = now() - timedelta(hours=24)
        if parser.parse(resp2[i]['created_at']) < time_threshold:
            del resp2[i]
            continue
        if 'retweeted_status' in resp2[i]:          # if retweeted => gets original tweet
            resp2[i]=resp2[i]['retweeted_status']
        if (not resp2[i]['entities']['urls']) or (not resp2[i]['lang'] == 'en'):
            del resp2[i]
            continue
        i += 1
    #user.usersettings.updated_storyfetch=last_time
    return resp2

@shared_task
def new_stories_from_feed(feed_id):
    #feed = serializers.deserialize("json", feed).next().object
    feed = Feed.objects.get(id=feed_id)
    feed.total_updates += 1
    last_updated = feed.last_updated
    feed.last_updated = now()
    feed.queued = False
    feed.save()
    try:
        d = feedparser.parse(feed.url)
    except SoftTimeLimitExceeded:
        logger.info('time limit exceeded for new_stories_from_feed feed = ', feed_id)
        logger.error("Could not fetch feed " + feed.url)
        feed.total_updates += 1
        feed.last_updated = now()
        feed.queued = False
        feed.save()
        return
    except:
        logger.error("Could not fetch feed " + feed.url)
        feed.total_updates += 1
        feed.last_updated = now()
        feed.queued = False
        feed.save()
        return
    #time_threshold = now() - timedelta(hours=150)
    time_threshold = now() - timedelta(hours=75)#Reducing time_thres to bring down refresh feeds load
    if last_updated:
        #time_threshold = max(feed.last_updated, time_threshold)
        time_threshold = max(last_updated, time_threshold)
    #q = django_rq.get_queue('save_story')
    for item in d.entries:
        if not (hasattr(item,'link') and hasattr(item,'title') and hasattr(item,'published_parsed') and item.published_parsed ) or item.title=='':
            continue
        item.link = item.link.strip('/')
        pub_time = make_aware(datetime.utcfromtimestamp(mktime(item.published_parsed)), pytz.UTC)
        if pub_time < time_threshold or pub_time > now() + timedelta(hours=24):
            continue
        title = strip_suffix(item.title,feed.website.domain.split('.')[0])
        if not story_new_and_valid(title,item.link,None,feed.commercial):
            create_manytomany_relation(feed, title, item.link)
            continue
        #serialized_feed_obj = serializers.serialize('json', [ feed, ])
        #save_story.apply_async(priority=2,args=[item.link,'feed', pub_time.isoformat(), title, 0, serialized_feed_obj], queue='save_story')
        #serialized_feed_obj = serializers.serialize('json', [ feed, ])
        #retrieve_url.apply_async(args=[item.link,'feed', pub_time.isoformat(), title, 0, feed_id], queue='network_queue')
        save_story.apply_async(args=[item.link,'feed', pub_time.isoformat(), title, 0, feed_id], queue='save_story')
        #q.enqueue(save_story,item.link,'feed', pub_time,title,0,feed, timeout=60)
    

def fetch_all_feeds_for_user(user_id):
    user = User.objects.get(id=user_id)
    all_user_feed_ids = ContentStreamRss.objects.filter(user=user).values_list('user_feed', flat=True)
    for feed_id in all_user_feed_ids:
        new_stories_from_commercial_feed(feed_id, immediate_fetch=True)

@shared_task
def new_stories_from_commercial_feed(feed_id, immediate_fetch=False):
    feed = UserFeed.objects.get(id=feed_id)
    feed.queued = False
    feed.total_updates += 1
    feed.last_updated = now()
    feed.save()
    #feed = serializers.deserialize("json", feed).next().object
    try:
        d = feedparser.parse(feed.url)
    except SoftTimeLimitExceeded:
        logger.info('time limit exceeded for new_stories_from_commercial_feed feed = ', feed_id)
        return
    except:
        logger.error("Could not fetch feed " + feed.url)
        return
    """
    if feed.items_fetched == 0:
        time_threshold = False
        entries = d.entries[:10]
    else:
        time_threshold = now() - timedelta(hours=36)
        entries = d.entries
    """
    entries = d.entries
    """
    if feed.last_updated and time_threshold:
        time_threshold = max(feed.last_updated, time_threshold)
    """
    #q = django_rq.get_queue('save_story')
    for item in entries[:30]:
        if not (hasattr(item,'link') and hasattr(item,'title') and hasattr(item,'published_parsed') and item.published_parsed ) or item.title=='':
            continue
        item.link = item.link.strip('/')
        pub_time = make_aware(datetime.utcfromtimestamp(mktime(item.published_parsed)), pytz.UTC)
        """
        if time_threshold:
            if pub_time < time_threshold or pub_time > now() + timedelta(hours=24):
                continue
        """
        title = strip_suffix(item.title,feed.website.domain.split('.')[0])
        """
        if not story_new_and_valid(title,item.link,None,feed.commercial):
            create_manytomany_relation(feed, title)
            continue
        """
    # if story already exists, connect it with this feed and skip saving again
        if create_manytomany_relation_user_feed_table(feed, title, item.link):
            continue

        #save_commercial_story.apply_async(priority=2,args=[item.link,pub_time.isoformat(),serialized_feed_obj], queue='save_story')
        #serialized_feed_obj = serializers.serialize('json', [ feed, ])
        #retrieve_url.apply_async(args=[item.link, 'commercial_feed', pub_time.isoformat(), title, 0, feed.id], queue='network_queue')
        if immediate_fetch:
            save_commercial_story(item.link,pub_time.isoformat(),feed_id, title=title)
        else:
            if feed.id % 2 == 0:
                save_commercial_story.apply_async(args=[item.link,pub_time.isoformat(),feed_id], kwargs={'title':title}, queue='save_commercial_story_new')
            else:
                save_commercial_story.apply_async(args=[item.link,pub_time.isoformat(),feed_id], kwargs={'title':title}, queue='save_commercial_story_new_2')
        #q.enqueue(save_commercial_story, item.link, pub_time, feed, timeout=60)
    


@shared_task()
def trim_save_story_queue():
    tm= now() - timedelta(hours=3*24)
    SaveStoryQueue.objects.filter(timestamp__lt=tm).delete()

def create_manytomany_relation_user_feed_table(feed, title, url):
    title_lower = title.lower() if len(title) < 195 else title[:190].lower() + ' ...'
    url_variants = [url, url.strip('/'), url+'/']
    #title_lower_variants = [title_lower, title_lower.strip(' ...'), title_lower + ' ...']
    s = StoryUserFeed.objects.filter(Q(url__in=url_variants) | Q(title_lower=title_lower)).first()
    if not s:
        url_copies = UrlCopiesUserFeed.objects.filter(url__in=url_variants)
        if url_copies.exists():
            original_url = url_copies.first().original_url
            s = original_url.story_user_feed
            if s:
                s.user_feed.add(feed)
                return True
        return False
    s.user_feed.add(feed)
    return True
    """
    if StoryUserFeed.objects.filter(url=url).exists():
        if not StoryUserFeed.objects.filter(url=url, user_feed=feed).exists():
            s = StoryUserFeed.objects.filter(url=url).first()
            s.user_feed.add(feed)
        return True
    if StoryUserFeed.objects.filter(url=url.strip('/')).exists():
        if not StoryUserFeed.objects.filter(url=url.strip('/'), user_feed=feed).exists():
            s = StoryUserFeed.objects.filter(url=url.strip('/')).first()
            s.user_feed.add(feed)
        return True
    if StoryUserFeed.objects.filter(url=url + '/').exists():
        if not StoryUserFeed.objects.filter(url=url + '/', user_feed=feed).exists():
            s = StoryUserFeed.objects.filter(url=url + '/').first()
            s.user_feed.add(feed)
        return True
    if StoryUserFeed.objects.filter(title_lower=title.lower()).exists():
        if not StoryUserFeed.objects.filter(title_lower=title.lower(), user_feed=feed).exists():
            s = StoryUserFeed.objects.filter(title_lower=title.lower()).first()
            s.user_feed.add(feed)
        return True
    return False
    """

def create_manytomany_relation(feed, title, url):
    if Story.objects.filter(url=url).exists():
        if not Story.objects.filter(url=url, feeds=feed).exists():
            s = Story.objects.filter(url=url).first()
            s.feeds.add(feed)
        return True
    if Story.objects.filter(title=title).exists():
        if not Story.objects.filter(title=title, feeds=feed).exists():
            s = Story.objects.filter(title=title).first()
            s.feeds.add(feed)
        return True
    return False


def check_integrity_error(feed, url):
    if StoryUserFeed.objects.filter(url=url).exists():
        if not StoryUserFeed.objects.filter(url=url, feeds=feed).exists():
            s = StoryUserFeed.objects.filter(url=url).first()
            s.feeds.add(feed)
    elif Story.objects.filter(url=url).exists():
        if not Story.objects.filter(url=url, feeds=feed).exists():
            s = Story.objects.filter(url=url).first()
            s.feeds.add(feed)
    return

def check_integrity_error_user_feed(feed, url):
    if StoryUserFeed.objects.filter(url=url).exists():
        if not StoryUserFeed.objects.filter(url=url, user_feed=feed).exists():
            s = StoryUserFeed.objects.filter(url=url).first()
            s.user_feed.add(feed)
    return
