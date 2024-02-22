import facebook_scraper as fs
import pandas as pd
import re
from copy import deepcopy
from facebook_scraper import *
from facebook_scraper import _scraper
from facebook_scraper import get_posts
from pyArango.connection import *
from constants import *


def extract_facebook_post(post_url, options, timeout=60):
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


def extract_important_fields(post):
    """select subset of all post keys based on constant IMPORTANT_KEYS

    :param post: post data
    :type post: dict
    :return: important post data
    :rtype: dict
    """
    return dict((k, post[k]) for k in IMPORTANT_KEYS)


def generate_doc_key(link):
    """regex search for FB user_id from a link

    :param link: facebook profile link
    :type link: string
    :return: FB user_id
    :rtype: string
    """
    try:
        return re.search('id=(.*)&fref', link).group(1)
    except:
        return re.search('com/(.*)?fref', link).group(1).replace('.', '_').replace(' ', '_').replace('?', '').lower()


def insert_document(collection, dict_doc):
    """insert a document into collection on ArangoDB

    :param collection: specific collection to insert doc into
    :type collection: ArangoDB collection
    :param dict_doc: other metadata to insert into document
    :type dict_doc: dict
    :return: Node in collection
    :rtype: ArangoDB document
    """
    doc = collection.createDocument()
    print(f'Document initiated')

    for k, v in dict_doc.items():
        doc[k] = v
    print('Document written')

    doc.save()
    print(f'{doc._key} saved to database.')

    return doc


def patch_document(collection, doc_key, dict_doc):
    """patch a document found in a collection on ArangoDB

    param collection: specific collection to insert doc into
    :type collection: ArangoDB collection
    :param doc_key: _key of document
    :type doc_key: string
    :param dict_doc: other metadata to insert into document
    :type dict_doc: dict
    :return: Node in collection
    :rtype: ArangoDB document
    """
    doc = collection.fetchDocument(doc_key)
    print(f'Document {doc._key} retrieved')

    for k, v in dict_doc.items():
        doc[k] = v
    print('Document written')

    doc.patch()
    print(f'{doc._key} patched.')

    return doc


def load_database(connection, db_name):
    """load ArangoDB

    :param connection: required connection to ArangoDB
    :type connection: ArangoDB connection object
    :param db_name: name of database
    :type db_name: string
    :return: database
    :rtype: ArangoDB database
    """
    if connection.hasDatabase(db_name) == True:
        db = Database(connection=connection, name=db_name)
        print(f'{db_name} database loaded.')
    else:
        print(f'{db_name} database does not exist. Will create.')
        db = connection.createDatabase(name=db_name)
        print(f'{db_name} database created.')
    return db


def load_collection(db, collection_name, document=1):
    """load a node/edge collection

    :param db: database in ArangoDB
    :type db: ArangoDB database object
    :param collection_name: name of collection to load
    :type collection_name: string
    :param document: if node or edge, defaults to 1
    :type document: int, optional
    :return: collection needed
    :rtype: ArangoDB collection
    """
    if document == 1:  # if document
        if db.hasCollection(collection_name) == True:
            collection = db.collections[collection_name]
            print(f'{collection_name} collection loaded.')
        else:
            print(f'{collection_name} collection does not exist. Will create.')
            collection = db.createCollection(name=collection_name)
            print(f'{collection_name} collection created.')
    else:  # if edge
        if db.hasCollection(collection_name) == True:
            collection = db.collections[collection_name]
            print(f'{collection_name} collection loaded.')
        else:
            print(f'{collection_name} collection does not exist. Will create.')
            collection = db.createCollection("Edges", name=collection_name)
            print(f'{collection_name} collection created.')
    return collection


def create_list_of_node_stores(collection):
    """create list of all document metadata in an ArangoDB collection

    :param collection: collection to query
    :type collection: ArangoDB collection object
    :return: list of all document metadata
    :rtype: list of dicts
    """
    nodes = []
    for x in collection.fetchAll():
        nodes.append(x.getStore())
    return nodes


def search_for_existing_node(list_of_node_stores, search_param, search_term, verbose):
    """used to determine if create new doc or patch existing doc

    :param list_of_node_stores: list of all document metadata in an ArangoDB collection
    :type list_of_node_stores: list of dicts
    :param search_param: which parameter to filter on
    :type search_param: string
    :param search_term: term to check in param
    :type search_term: string
    :param verbose: 1 to print results, 0 to not
    :type verbose: int
    :return: list of results
    :rtype: list
    """
    print(f'Searching for the ff:')
    print(f'{search_param} => {search_term}')
    result = list(filter(lambda item: item[search_param] == search_term, list_of_node_stores))
    if verbose == 1:
        print(f'Results: {result}')
    return result


def search_for_existing_edge(
    list_of_node_stores,
    search_param_from,
    search_term_from,
    search_param_to,
    search_term_to,
    search_param_addtl,
    search_term_addtl,
    verbose=0,
):
    """_summary_

    :param list_of_node_stores: list of all document metadata in an ArangoDB collection
    :type list_of_node_stores: list of dicts
    :param search_param_from: which parameter to filter on for from_node
    :type search_param_from: string
    :param search_term_from: term to check in param for from_node
    :type search_term_from: string
    :param search_param_to: which parameter to filter on for to_node
    :type search_param_to: string
    :param search_term_to: term to check in param for to_node
    :type search_term_to: string
    :param search_param_addtl: additonal params
    :type search_param_addtl: string
    :param search_term_addtl: term to check in additional params
    :type search_term_addtl: string
    :param verbose: 1 to print results, 0 to not, defaults to 0
    :type verbose: int, optional
    :return: list of results
    :rtype: list
    """
    print(f'Searching for the ff:')
    print(f'{search_param_from} => {search_term_from}')
    print(f'{search_param_to} => {search_term_to}')
    print(f'{search_param_addtl} => {search_term_addtl}')
    result = list(
        filter(
            lambda item: (
                item[search_param_from] == search_term_from
                and item[search_param_to] == search_term_to
                and item[search_param_addtl] == search_term_addtl
            ),
            list_of_node_stores,
        )
    )
    if verbose == 1:
        print(f'Results: {result}')
    return result


def assess_edge_search_result(
    result, edge_collection, from_collection_name, to_collection_name, from_node_doc, to_node_doc, dict_edge
):
    """determine if patch or create new edge

    :param result: list of existing docs
    :type result: list
    :param edge_collection: edge collection on ArangoDB
    :type edge_collection: ArangoDB collection object
    :param from_collection_name: from collection on ArangoDB
    :type from_collection_name: ArangoDB collection object
    :param to_collection_name: to collection on ArangoDB
    :type to_collection_name: ArangoDB collection object
    :param from_node_doc: document with metadata
    :type from_node_doc: ArangoDB document object
    :param to_node_doc: document with metadata
    :type to_node_doc: ArangoDB document object
    :param dict_edge: additional properties to add to edge
    :type dict_edge: dict
    :return: edge that was built
    :rtype: ArangoDB edge object
    """
    if len(result) > 0:
        key_label = result[0]['_key']
        edge = result[0]  # dict
        print(f'Found existing edge {key_label}')
    else:
        # insert new document
        key_label = None
        print(f'No existing edge. Will insert new edge.')
        edge = edge_collection.createEdge()
        for k, v in dict_edge.items():
            edge[k] = v
        print('Edge properties written')
        edge.links(f'{from_collection_name}/{from_node_doc["_key"]}', f'{to_collection_name}/{to_node_doc["_key"]}')
        edge.save()
        print(f'Created edge {edge._key}')
    return edge


def assess_node_search_result(result, collection, dict_data):
    """determine if patch or create new node

    :param result: list of existing docs
    :type result: list
    :param collection: collection on ArangoDB
    :type collection: ArangoDB collection object
    :param dict_data: additional properties to add to node
    :type dict_data: dict
    :return: doc that was built
    :rtype: ArangoDB document object
    """
    if len(result) > 0:
        key_label = result[0]['_key']
        print(f'Found existing node {key_label}')
        doc = patch_document(collection, key_label, dict_data)
    else:
        # insert new document
        print(f'No existing node. Will insert new node.')
        doc = insert_document(collection, dict_data)
        print(f'Created node {doc["_key"]}')
    return doc


def backfill_posters_collection_from_post_docs(posts_collection, posters_collection):
    """helper function just in case we miss out on adding posters of posts into ArangoDB

    :param posts_collection: collection of posts
    :type posts_collection: ArangoDB collection object
    :param posters_collection: collection of posters
    :type posters_collection: ArangoDB collection object
    :return: list of poster documents
    :rtype: list
    """
    list_of_post_nodes = create_list_of_node_stores(posts_collection)
    list_of_poster_nodes = create_list_of_node_stores(posters_collection)
    poster_docs = []

    for retrieved_post_doc in list_of_post_nodes:
        print(retrieved_post_doc['username'])
        poster_profile = {
            'user_id': retrieved_post_doc['user_id'],
            'username': retrieved_post_doc['username'],
            'link': f"https://facebook.com/profile.php?id={retrieved_post_doc['user_id']}&fref=pb",
        }

        result = search_for_existing_node(
            list_of_poster_nodes, search_param='user_id', search_term=retrieved_post_doc['user_id']
        )
        poster_doc = assess_node_search_result(result, posters_collection, poster_profile)
        poster_docs.append(poster_doc)
    return poster_docs
