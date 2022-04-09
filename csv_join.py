import pandas as pd

import argparse

def get_args_parser():
    parser = argparse.ArgumentParser("Join two csv files with custom join type")
    parser.add_argument(
        "--path_first",
        help="path to the first csv file",
    )
    
    parser.add_argument(
        "--primarykey_first",
        help="primary key in the first file",
    )
    
    parser.add_argument(
        "--path_second",
        help="path to the second csv file",
    )
    
    parser.add_argument(
        "--primarykey_second",
        help="primary key in the first file",
    )
    
    parser.add_argument(
        "--join_type",
        help="join type, can be left/right/inner, default is left",
        default="left"
    )
    
    return parser


def left_join(file1, file2, pk1, pk2):
    """Fuction to create csv file merging file1 and file2 with left join type

    Args:
        file1 (pd.DataFrame):  first file to join
        file2 (pd.DataFrame): second file to join
        pk1 (str): first file table primary key
        pk2 (str): second file table primary key
    """
    header1 = list(file1.columns.values)
    header2 = list(file1.columns.values)

   
    pk1_column = header1.index(pk1)
    pk2_column = header2.index(pk2)


            
    for row in file1:
        print(list(row),end=", ")
        file1_pk_val = row[pk1_column]
        if file1_pk_val in file2[pk2]:
            index = file2[pk2].index(file1_pk_val)
            print(list(file2.loc[[index]]))
        else:
            print("None, " * (len(header2)-1),end="") 
            print("None") 
            
def right_join(file1, file2, pk1, pk2):
    """Fuction to create csv file merging file1 and file2 with right join type

    Args:
        file1 (pd.DataFrame):  first file to join
        file2 (pd.DataFrame): second file to join
        pk1 (str): first file table primary key
        pk2 (str): second file table primary key
    """
    header1 = list(file1.columns.values)
    header2 = list(file1.columns.values)

   
    pk1_column = header1.index(pk1)
    pk2_column = header2.index(pk2)


            
    for row in file2:
        
        file2_pk_val = row[pk2_column]
        if file2_pk_val in file1[pk1]:
            index = file1[pk1].index(file2_pk_val)
            print(list(file1.loc[[index]]),end=", ")
        else:
            print("None, " * (len(header2))) 
                      
        print(list(row))   
        
        
def inner_join(file1, file2, pk1, pk2):
    """Fuction to create csv file merging file1 and file2 with inner join type

    Args:
        file1 (pd.DataFrame):  first file to join
        file2 (pd.DataFrame): second file to join
        pk1 (str): first file table primary key
        pk2 (str): second file table primary key
    """
    header1 = list(file1.columns.values)
    header2 = list(file1.columns.values)

   
    pk1_column = header1.index(pk1)
    pk2_column = header2.index(pk2)


            
    for row in file2:
        
        file2_pk_val = row[pk2_column]
        if file2_pk_val in file1[pk1]:
            index = file1[pk1].index(file2_pk_val)
            print(list(file1.loc[[index]]),end=", ")
            print(list(row)) 
                      
            
    



if __name__ == "__main__":
    parser = get_args_parser()
    args = parser.parse_args()
    
    file1 = args.path_first
    file2 = args.path_second
    
    file1 = pd.pd.read_csv(file1)
    file2 = pd.pd.read_csv(file2)
    join_type = args.join_type
    
    pk1 = args.primarykey_first
    pk2 = args.primarykey_second
    
    if join_type == "left":
        left_join(file1, file2, pk1, pk2)
    elif join_type == "right":
        right_join(file1, file2, pk1, pk2)
    else:
        inner_join(file1, file2, pk1, pk2)
        
    