import pandas as pd
from typing import List, Callable, Text, Any, Union
from pprint import pprint
import os
import time 

"""
主要目标纬度：
- TOTAL GFA (sqm)
- TOTAL Monthly Rental 
- Financial Occupancy rate 
- Physical Occupancy rate
- 租金单价（合同租金）
- 租金单价（基于Physical Occ）
- Commission
"""

class RR_Summary:
    def __init__(self, file_path:str, sheet_name_1:str, sheet_name_2:str)-> None:
        self.file_path= file_path
        self.sheet_name_1= sheet_name_1
        self.sheet_name_2= sheet_name_2
        self.data= self.get_data()
        self.path= "processed_data"
        
    def get_data(self, tab:int=1)->pd.DataFrame:
        if tab==1:
            return pd.read_excel(self.file_path, self.sheet_name_1)
        return pd.read_excel(self.file_path, self.sheet_name_2)

    def process_rr_summary(self)->pd.DataFrame:
        summary= self.data.iloc[6:32, :]
        summary.columns= self.data.iloc[5,:].values
        rr_summary=summary.fillna("0")
        rr_summary= rr_summary.reset_index()
        rr_summary.drop("index", axis=1, inplace= True)
        rr_summary= rr_summary.drop(columns=rr_summary.columns[3:12], axis=1)
        rr_summary.to_csv(os.path.join(self.path, "rr_summary_fact_table.csv"), index= False)

        return rr_summary


    def get_cur2future_rental(self)->pd.DataFrame:
        cur2future= self.data.iloc[:4,12:26]
        cur2future.rename(columns={"Unnamed: 12":""}, inplace= True)
        update_cur2future= cur2future.set_index(  '').T
        update_cur2future=update_cur2future.reset_index().rename(columns={"index":"Year"})
        
        return update_cur2future

    # 物业平均信息
    def get_avg(self)->pd.DataFrame:
        rr_avg=self.data.iloc[34:45, :2]
        rr_avg.dropna(subset=['Unnamed: 0'], inplace= True)
        rr_avg.rename(columns={'Unnamed: 0':'properties', 'Unnamed: 1':'value'}, inplace= True)
        rr_avg.to_csv(os.path.join(self.path, "avg_rr.csv"), index= False)

        return rr_avg

    # 新租、续租平均租金
    def avg_rental(self)->pd.DataFrame:
        rental_avg=self.data.iloc[48:55, :2]
        rental_avg.columns=["Year", "Avg_Commission"]
        rental_avg.to_csv(os.path.join(self.path, "rental_avg.csv"), index= False)

        return rental_avg

    # 到期面积
    def get_expired_area(self)->pd.DataFrame:
        expired_area=self.data.iloc[58:, :2]
        expired_area.columns= ["Year", "Expired_Area"]
        expired_area.to_csv(os.path.join(self.path, "expired_area.csv"), index= False)

        return expired_area

    def get_rent(self, start_row, end_row, rent_type:str="MKT")->pd.DataFrame:
        df= self.get_data(2)
        df= df.iloc[list(range(start_row, end_row)), :20].fillna("0")
        df.drop(columns=['Unnamed: 1','Unnamed: 3'], inplace= True)
        df= df.set_index('Unnamed: 0')
        df= df.T
        df["Year"]= df["Year"].apply(lambda x: int(x))
        df= df.set_index("Year")
        df.rename(columns={df.columns[0]:f"Rental_{rent_type}"}, inplace= True)
        
        return df

    # combine rr_summary fact table with forecast(maybe)
    # TODO
        """
         Issue-> 因为在excel `JQ L7 Rent Roll`表格中有很多部分数据没有显示出来，所以作为展示，就拿取了`Revenue` 
         预测部分数据。其余待数据梳理后，再带入函数输出即可。
        """
    
    @staticmethod
    def combine(df):
        # 以资产名_单元号_code，作为unique主键
        return str(df['Unnamed: 0']) + " " + str(df['Unnamed: 1'])+ " "+ str(df['Unnamed: 2'])

    def get_rent_forecast_table(self, func: Callable,
                            row_start:int, 
                            row_end:int, 
                            col_start:int=109, 
                            col_end:int=109+(12*8))->pd.DataFrame:
        
        df= self.get_data(2)
        df= df.iloc[list(range(row_start, row_end)), :].reset_index()
        df= df.drop(columns='index')
        df["RR_UNIT"]= df.apply(func, axis=1)
        df_update= df.iloc[:, list(range(col_start, col_end))]
        df_update["RR_UNIT"]= df["RR_UNIT"]
        df_update.columns= df_update.iloc[0, :]
        df_update= df_update.iloc[2:]
        df_update.to_csv(os.path.join(self.path, "revenue_forecast_table.csv"), index= False)

        return df_update
        
        
def count_time(f:Callable)->Text:
    start= time.time()
    f()
    till_end= time.time()-start
    pprint(f"Total cost is {till_end}")


def main()->Union[Text, Any]:
    PATH= "processed_data"
    rr= RR_Summary("1. L7 BP V3_20191225 - 副本.xlsx", "RR summary", "JQ L7 Rent Roll")
    rr_summary= rr.process_rr_summary()
    rr_summary_1= rr_summary.iloc[:, [0,1,2,3,5,6, 7, 8, 9,10, -3,-2,-1]]
    rr_summary_1.to_csv(os.path.join(PATH, "rr_summary_1.csv"), index= False)
    cur2future= rr.get_cur2future_rental()
    GF_Retail_mkt_rent= rr.get_rent(13,15)
    Office_mkt_rent= rr.get_rent(9,11, "OFFICE")
    
    total_rent= Office_mkt_rent.merge(GF_Retail_mkt_rent, on="Year")\
        .merge(cur2future, on="Year")
    total_rent.to_csv(os.path.join(PATH, "total_rent.csv"), index= False)
    
    rr_avg= rr.get_avg()
    rental_avg= rr.avg_rental()
    forecast_revenue_table= rr.get_rent_forecast_table(rr.combine, 17,45)
    print(forecast_revenue_table)
    
    expired_area= rr.get_expired_area()

    

if __name__=="__main__":
    # 资产一览
    count_time(main)
    
