import glob
import uuid
import os
import pandas as pd
import json

def get_columns(ds):
    schemas_file_path=os.environ.setdefault('SCHEMAS_FILE_PATH','C:/Users/Ganesh Naidu/AppData/Local/Programs/Python/iam-converter/data/retail_db/schemas.json')
    with open(schemas_file_path) as fp:
        schemas= json.load(fp)

    try:
        schema=schemas.get(ds)
        if not schema:
            raise KeyError
        cols = sorted(schema,key=lambda s:s['column_position'])
        columns =[col['column_name']for col in cols]
        return columns
    except KeyError:
        print(f'Schema not found for {ds}')
        return    


def process_file(src_base_dir,ds,tgt_base_dir):

     for file in glob.glob(f'{src_base_dir}/{ds}/part*'):

        df = pd.read_csv(file, names=get_columns(ds))

        os.makedirs(f'{tgt_base_dir}/{ds}', exist_ok=True)

        df.to_json(

            f'{tgt_base_dir}/{ds}/part-{str(uuid.uuid1())}.json',

            orient='records',

            lines=True

         )
        print(f'Number of records processed for {os.path.split(file)[1]} in {ds} is {df.shape[0]}')

if __name__=='__main__':
    print(get_columns('departments'))
    import json



def main():
    src_base_dir =os.environ['SRC_BASE_DIR']
    tgt_base_dir = os.environ['TGT_BASE_DIR']
    datasets = os.environ.get('DATASETS')
    if not datasets:

       for path in glob.glob(f'{src_base_dir}/*'):

           if os.path.isdir(path):

               process_file(src_base_dir,os.path.split(path)[1],tgt_base_dir)
    else:
        dirs=datasets.split(',')
        for ds in dirs:
             process_file(src_base_dir,ds,tgt_base_dir)

                  
 

 

if __name__ == '__main__':

    main()

