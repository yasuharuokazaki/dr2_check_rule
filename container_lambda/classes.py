import sys
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import List
from dataclasses import replace
import re
import itertools

@dataclass
class TriggerRow:
    index:str = ""
    title:str = ""
    description:str = ""


class TriggerTable:
    def __init__(self,sheet_df:pd.DataFrame)->None:
        """
        Arg:
            sheet_df:サンプルエクセルファイルをDataFrameに変換したもの
        """
        self.pj_rows:List[TriggerRow]=[]
        self.ot_rows:List[TriggerRow]=[]
        self.in_rows:List[TriggerRow]=[]
        self.fs1_rows:List[TriggerRow]=[]
        self.fs2_rows:List[TriggerRow]=[]
        
        self.extra_pj(sheet_df)
        self.extra_ot(sheet_df)
        self.extra_in(sheet_df)
        self.extra_fs1(sheet_df)
        self.extra_fs2(sheet_df)

    #PJリスト作成
    def extra_pj(self,sheet_df):
        pj_df = sheet_df[sheet_df["項目index"].str.contains("PJ")]
        self.pj_rows = [ TriggerRow(index,title,description) for index,title,description in zip(pj_df["項目index"].values,pj_df['項目'].values,pj_df['内容'].values)]
        
    
    #OTリスト作成
    def extra_ot(self,sheet_df):
       ot_df = sheet_df[sheet_df["項目index"].str.contains("OT")]
       self.ot_rows = [ TriggerRow(index,title,description) for index,title,description in zip(ot_df["項目index"].values,ot_df["項目"].values,ot_df["内容"].values)]
       
    #INリスト作成
    def extra_in(self,sheet_df):
       in_df = sheet_df[sheet_df["項目index"].str.contains("IN")]
       self.in_rows = [ TriggerRow(index,title,description) for index,title,description in zip(in_df["項目index"].values,in_df["項目"].values,in_df["内容"].values)]
       
    
    #FSリスト作成
    def extra_fs1(self,sheet_df):
    #    presence_or_absence_items = fs_presence_or_absence_list
       fs1_df = sheet_df[sheet_df["項目index"].str.contains("FS-01")]
       self.fs1_rows = [TriggerRow(index,title,description) for index,title,description 
                      in zip(fs1_df["項目index"].values,fs1_df["項目"].values,fs1_df["内容"].values)
                    #   if index in presence_or_absence_items and description == 1
                      ]
    def extra_fs2(self,sheet_df):
    #    presence_or_absence_items = fs_presence_or_absence_list
       fs2_df = sheet_df[sheet_df["項目index"].str.contains("FS-02")]
       self.fs2_rows = [TriggerRow(index,title,description) for index,title,description 
                      in zip(fs2_df["項目index"].values,fs2_df["項目"].values,fs2_df["内容"].values)]


                      
@dataclass
class CheckRule:
    name:str = ""
    fs1_condition:any=""
    fs2_condition:any=""
    pj_condition:any=""
    ot_condition:any=""
    in_condition:any=""
    flag_num:int = 0
    hit_triggers:any=""

class CheckList:
    def __init__(self,DF:pd.DataFrame)->None:
        df = DF.copy()
        self.check_rules:List[CheckRule] = ""
        self.check_rules:List[CheckRule] = [CheckRule(name,fs1_condition,fs2_condition,pj_condition,ot_condition,in_condition) for name,fs1_condition,fs2_condition,pj_condition,ot_condition,in_condition
                                           in zip(df["失敗に学ぶ"].values,df['FS1'].values,df['FS2'].values,df['PJ'].values,df['OT'].values,df['IN'].values)]
        for checkRule in self.check_rules:
            if(checkRule.fs1_condition != "" and
               checkRule.fs1_condition != "ー" and
               checkRule.fs1_condition != None and
               str(checkRule.fs1_condition) != "nan"):
                checkRule.flag_num += 1
            if(checkRule.fs2_condition != "" and
               checkRule.fs2_condition != "ー" and
               checkRule.fs2_condition != None and
               str(checkRule.fs2_condition) != "nan"):
                checkRule.flag_num += 1
            if(checkRule.pj_condition != "" and
               checkRule.pj_condition != "ー" and
               checkRule.pj_condition != None and
               str(checkRule.pj_condition) != "nan"):
                checkRule.flag_num += 1
            if(checkRule.in_condition != "" and
               checkRule.in_condition != "ー" and
               checkRule.in_condition != None and
               str(checkRule.in_condition) != "nan"):
                checkRule.flag_num += 1
            if(checkRule.ot_condition != "" and
               checkRule.ot_condition != "ー" and
               checkRule.ot_condition != None and
               str(checkRule.ot_condition) != "nan"):
                checkRule.flag_num += 1
        
    def fs1_conditions(self):  
        return [ fs_rule if fs_rule != None else fs_rule for fs_rule in self.check_rules 
                if fs_rule.fs1_condition != "" and fs_rule.fs1_condition != "ー" and fs_rule.fs1_condition != None
                and str(fs_rule.fs1_condition) != "nan" ]
    
    def fs2_conditions(self):
        return [fs_rule for fs_rule in self.check_rules 
                if fs_rule.fs2_condition != "" and fs_rule.fs2_condition != "ー" and fs_rule.fs2_condition != None
                and str(fs_rule.fs2_condition) != "nan"]

    def pj_conditions(self):
        return [pj_rule for pj_rule in self.check_rules if pj_rule.pj_condition != "ー" and pj_rule.pj_condition != None
                and str(pj_rule.pj_condition) != "nan"]
    
    def in_conditions(self):
        return [in_rule for in_rule in self.check_rules if in_rule.in_condition != "ー" and in_rule.in_condition != None
                and str(in_rule.in_condition) != "nan"]
    
    def ot_conditions(self):
        return [ot_rule for ot_rule in self.check_rules if ot_rule.ot_condition != "ー" and ot_rule.ot_condition != None
                and str(ot_rule.ot_condition) != "nan"]
        # self.ruleRows:List[RuleRow]=[ RuleRow(name,fs_condition,pj_condition,ot_condition,in_condition)
        #   for name,fs_condition,pj_condition,ot_condition,in_condition 
        #   in zip(df["失敗に学ぶ"].values,df['FS'].values,df['PJ'].values,df['OT'].values,df['IN'].values)]
        
        
