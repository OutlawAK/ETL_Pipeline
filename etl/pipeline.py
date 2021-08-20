
"""This data pipeline uses movie lens database. Takes MySQL data and puts it into mongoDB"""

# System libs
import copy

# External Libs
from pymongo import MongoClient
import pymysql

# Internal libs 
import config 


RESET_MONGO_COLLECTIONS_ON_UPDATE = True # Resets the collections if a collection already exists, if false, the data is appeneded to the collection
PRINT_INFO = True # Print options for debugging purposes
PRINT_RESULTS = True # Print option for debugging purposes

def initalise_mysql(): 
    """Initalises and returns a MySQL database based on config"""
    return pymysql.connect(
        host=config.MYSQL_HOST,
        user=config.MYSQL_USERNAME,
        password=config.MYSQL_PASSWORD,
        db=config.MYSQL_DB)

def initalise_mongo():
    """Initalises and returns MongoDB database based on config"""
    return MongoClient(config.MONGO_HOST, config.MONGO_PORT)[config.MONGO_DB]

def extract_data(mysql_cursor):  
    """Given a cursor, Extracts data from MySQL movielens dataset
    and returns all the tables with their data"""
    genome_tags = execute_mysql_query('select * from genome_tags', mysql_cursor, 'fetchall')
    tags = execute_mysql_query('select * from tags', mysql_cursor, 'fetchall')
    movies = execute_mysql_query('select * from movies', mysql_cursor, 'fetchall')
    ratings = execute_mysql_query('select * from ratings', mysql_cursor, 'fetchall')
    genome_scores = execute_mysql_query('select * from genome_scores', mysql_cursor, 'fetchall')
    tables = (genome_tags, tags, ratings, genome_scores, movies)
    return tables

def execute_mysql_query(sql, cursor, query_type):
    """exectues a given sql, pymysql cursor and type"""
    if query_type == "fetchall":
        cursor.execute(sql)
        return cursor.fetchall()
    elif query_type == "fetchone":
        cursor.execute(sql)
        return cursor.fetchone() 
    else:
        pass

def transform_data(dataset, table):
    """Transforms the data to load it into mongoDB, returns a JSON object"""
    dataset_collection = []
    tmp_collection = {}
    if table == "genome_tags":
        for item in dataset[0]:
            tmp_collection['tagId'] = item[0]
            tmp_collection['genre'] = item[1]
            dataset_collection.append(copy.copy(tmp_collection))
        return dataset_collection
    elif table == "genome_scores":
        for item in dataset[3]:
            tmp_collection['movieId'] = item[0]
            tmp_collection['tagId'] = item[1]
            tmp_collection['relevance'] = item[2]
            dataset_collection.append(copy.copy(tmp_collection))
        return dataset_collection
    elif table == "movies":
        for item in dataset[4]:
            tmp_collection['movieId'] = item[0]
            tmp_collection['title'] = item[1]
            tmp_collection['genres'] = item[2]
            # embedding movie genome_tags
            tags_collection = []
            for tags_item in dataset[1]:
                if tags_item[0] == tmp_collection['movieId']:
                    tags_collection.append(copy.copy(tags_item[1]))
            tmp_collection['genome_tags'] = tags_collection
            # embedding ratings
            ratings_collection = []
            for ratings_item in dataset[2]:
                if ratings_item[1] == tmp_collection['movieId']:
                    tmp_ratings_collection = {}
                    tmp_ratings_collection['userId'] = ratings_item[0]
                    tmp_ratings_collection['rating'] = ratings_item[2]
                    tmp_ratings_collection['timestamp'] = ratings_item[3]
                    ratings_collection.append(copy.copy(tmp_ratings_collection))
            tmp_collection['ratings'] = ratings_collection
            dataset_collection.append(copy.copy(tmp_collection))
        return dataset_collection 

def load_data(mongo_collection, dataset_collection):
    """Loads the data into mongoDB and returns the results"""
    if RESET_MONGO_COLLECTIONS_ON_UPDATE:
        mongo_collection.delete_many({})
    return mongo_collection.insert_many(dataset_collection)

def main():
    """main method starts a pipeline, extracts data,
    transforms it and loads it into a mongo client"""
    if PRINT_INFO:
        print('Starting data pipeline')
        print('Initialising MySQL connection')
    mysql = initalise_mysql()
    
    if PRINT_INFO:
        print('MySQL connection Completed')
        print('Starting data pipeline stage 1 : Extracting data from MySQL')
    mysql_cursor = mysql.cursor()
    mysql_data = extract_data(mysql_cursor)
    
    if PRINT_INFO:
        print('Stage 1 completed! Data successfully extracted from MySQL')
        print('Starting data pipeline stage 2: Transforming data from MySQL for MongoDB')
        print('Transforming genome_tags dataset')
    genome_tags_collection = transform_data(mysql_data, "genome_tags")
    
    if PRINT_INFO:
        print('Successfully transformed genome_tags dataset')
        print('Transforming genome_scores dataset')
    genome_scores_collection = transform_data(mysql_data, "genome_scores")
    
    if PRINT_INFO:
        print('Successfully transformed genome_scores dataset')
        print('Transforming movies dataset')
    movies_collection = transform_data(mysql_data, "movies")
    
    if PRINT_INFO:
        print('Successfully transformed genome_scores dataset')
        print('Stage 2 completed! Data successfully transformed')
        print('Intialising MongoDB connection')
    mongo = initalise_mongo()
    
    if PRINT_INFO:
        print('MongoDB connection Completed')
        print('Starting data pipeline stage 3: Loading data into MongoDB')
    result = load_data(mongo['genome_tags'], genome_tags_collection)
    
    if PRINT_RESULTS:
        print('Successfully loaded genome_tags')
        print(result)
    result = load_data(mongo['genome_scores'], genome_scores_collection)
    
    if PRINT_RESULTS:
        print('Successfully loaded genome_scores')
        print(result)
    result = load_data(mongo['movies'], movies_collection)
    
    if PRINT_RESULTS:
        print('Successfully loaded genome_scores')
        print(result)
    
    if PRINT_INFO:
        print('Stage 3 completed! Data successfully loaded')
        print('Closing MySQL connection')
    mysql.close()
    if PRINT_INFO:
        print('MySQL connection closed successfully')
        print('Ending data pipeline')

if __name__ == "__main__":
    main()
