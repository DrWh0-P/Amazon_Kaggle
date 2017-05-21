
# -*- coding: utf-8 -*-
import os
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from bson  import Binary
from PIL import Image
import pandas as pd
from io import BytesIO
from time import time
import re
import tifffile as tiff
import pickle
import click
import logging

@click.command()
@click.argument('input_folder', type=click.Path(exists=True))
@click.argument('type')
def main(input_filepath,type):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

    
    client=MongoClient()
    db=client.data
    train=db.train
    test=db.test
    project_home = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    data_folder='/data/'+input_folder+'/'
    
    descr=pd.read_csv(project_home+'data/train.csv')
    
    i=0
    if type=='train':
        db_name=train
    else:
        db_name=test

    bulk=db_name.initialize_ordered_bulk_op()
    file_list=[f for f in os.listdir(project_home+data_folder)]
    for file_name in file_list:
        if not(re.search("[0-9]+",file_name)):
            continue
        time_now=time()
        try:
            im=tiff.imread(project_home+data_folder+file_name)
           
        except:
            print (file_name +" skipped ")
            j=j+1
            continue
        im_byte=Binary(pickle.dumps(im,protocol=2))
        tags=descr.ix[descr["image_name"]==file_name[:-4],"tags"]
        if not(tags.empty):
            tags=tags.values[0].split()
        else:
            tags=""
        document={"name":file_name ,
                  "type":file_name[-3:] ,
                  "tags":tags,
                  "image":im_byte
        }
        
        bulk.insert(document)
        i+=1              

        if i%3000==0:
            try:
                bulk.execute()     
            except BulkWriteError as bwe:                
                print(file_name+" had bulk write errors")
            bulk=db_name.initialize_ordered_bulk_op()
    bulk.execute()
    client.close()

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main(project_dir)

    
    



