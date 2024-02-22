# public
from copy import deepcopy
import facebook_scraper as fs
from facebook_scraper import *
from facebook_scraper import _scraper
from facebook_scraper import get_posts
import pandas as pd
import pickle
import re

# custom
from constants import *
from credentials import fb_credentials


FB_USER = fb_credentials['fb_user']
FB_PASS = fb_credentials['fb_pass']

def load_dict(pkl_filename):
    pkl_file = open(pkl_filename, 'rb')
    dict_to_load = pickle.load(pkl_file)
    print('# of entries', len(dict_to_load))
    pkl_file.close()
    return dict_to_load

def dump_dict(pkl_filename, dict_to_dump):
    output = open(pkl_filename, 'wb')
    pickle.dump(dict_to_dump, output)
    output.close()
    print('# of entries dumped', len(dict_to_dump))

def extract_facebook_post(post_url, options, timeout=60, credentials=(FB_USER, FB_PASS)):
    """Use facebook_scraper's get_posts fcn to scrape a post's metadata

    :param post_url: post identifier
    :type post_url: string
    :param options: dict of options for scraping
    :type options: dict
    :param timeout: sleep time between calls, defaults to 60
    :type timeout: int, optional
    :return: post metadata
    :rtype: dict
    """
    post_urls = [post_url]
    for post in fs.get_posts(post_urls=post_urls, options=options, timeout=timeout):

        print(post['post_id'], 'get')
    return post


def get_reactors(post, filter_negative=1, pro_bbm_post=0, obvious_troll=0):
    """using various filters, extract all relevant reactors

    :param post: post data
    :type post: dictionary
    :param filter_negative: depending on whether post is pro or not, list of reactions to consider, defaults to 1
    :type filter_negative: int, optional
    :param pro_bbm_post: if the post feels like it's pro or not, defaults to 0
    :type pro_bbm_post: int, optional
    :param obvious_troll: if post is obviously a troll post, defaults to 0
    :type obvious_troll: int, optional
    :return: list of reactors
    :rtype: list of dicts
    """
    if obvious_troll == 0:
        if pro_bbm_post == 1:
            negative_reactions = ['like', 'love', 'care', 'wow']
        else:
            negative_reactions = ['haha', 'angry']

        if filter_negative == 1:
            return [reactor for reactor in post['reactors'] if reactor['type'] in negative_reactions]
        else:
            return post['reactors']
    else:
        return post['reactors']


def pretty_print_df(post):
    """helper fcn to pretty print dict as pandas DF

    :param post: post data
    :type post: dict
    """
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None):
        display(pd.DataFrame.from_dict(post, orient='index'))


def get_reactions(post):
    """extract dictionary of all reactions and counts

    :param post: post data
    :type post: dict
    :return: all reactions and counts
    :rtype: dict
    """
    print(f'Total of {post["reaction_count"]} reactions')
    return post['reactions']

