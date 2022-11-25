from typing import List
from classes import CheckList, CheckRule, TriggerRow, TriggerTable
import re

def flatten_list(l):
    for el in l:
        if isinstance(el, list):
            yield from flatten_list(el)
        else:
            yield el

def list_filter(c:str,t:List[TriggerRow]):
    if('and' in c):
        print('and')
    elif('or' in c):
        print('or')
    else:
        index_l = list(set([ re.search('(.*)-00\d',trigger.index).group(1) for trigger in t if c in str(trigger.description)]))
        filtered_triggers = [trigger for trigger in t for index in index_l if index in str(trigger.index) ]
        # print(f'filtterd triggers {filtered_triggers}')
        return filtered_triggers
       
       
def trigger_pickup(trigger_table:TriggerTable,check_rule:CheckList)->List:
# print(trigger.fs1_rows)
    rules = check_rule.check_rules
    # print(len(rules))
    return_triggers = {}
    for rule in rules[1:]:
        return_triggers[rule.name]=[]
        ####################### fs1check #########################################
        if(rule.fs1_condition!='ー' and str(rule.fs1_condition)!= 'nan'):
            conditions = [item.replace('\n',"") if '\n' in str(item) else item for item in re.split("：",rule.fs1_condition)]
            remove_space_conditions = [ re.findall('(.*)\u3000',condition.replace('（',""))[0] if '\u3000' in str(condition) else condition for condition in conditions]
            remove_space2_conditions = [re.findall('(.*)\u3000',condition.replace('（',""))[0] if '\u3000' in str(condition) else condition for condition in remove_space_conditions]
            remove_space3_conditions = [re.findall('(.*)\u3000',condition.replace('（',""))[0] if '\u3000' in str(condition) else condition for condition in remove_space2_conditions]
            # print(f'{rule.name}:{remove_space3_conditions} fs1')
     
            hit_triggers = [ trigger for trigger in trigger_table.fs1_rows for condition in conditions if condition in str(trigger.title) and trigger.description!='0' and trigger.description != ""]
            for trigger in hit_triggers:
                #風要件含んでいる場合にチェックが必要なトリガー抽出
                if(trigger.index=='FS-01-307'):
                    for add_trigger in [trigger for trigger in trigger_table.fs1_rows if trigger.index=="FS-01-308" or trigger.index=="FS-01-309" or trigger.index=="FS-01-310"]:
                        hit_triggers.append(add_trigger)
                #外溝雨量トリガー削除
                if(trigger.index=='FS-01-369' or trigger.index=='FS-01-370' or trigger.index=='FS-01-371'):
                    hit_triggers = [trigger for trigger in hit_triggers if trigger.index!='FS-01-369' and trigger.index != 'FS-01-370' and trigger.index!='FS-01-371']
                   
            duplicate = []
            non_duplicate_hit_triggers = [hit_trigger for hit_trigger in hit_triggers if str(hit_trigger.description) != 'nan' and hit_trigger not in duplicate and not duplicate.append(hit_trigger)]
            # print(f'hit_triggers:{non_duplicate_hit_triggers}')
            return_triggers[rule.name]+=[trigger.index for trigger in non_duplicate_hit_triggers if str(trigger.description) != 'nan']
        ############### fs2 check #############################################################################
        if( rule.fs2_condition != 'ー' and str(rule.fs2_condition)!='nan'):
            conditions = [item.replace('\n',"") if '\n' in str(item) else item for item in re.split("）",rule.fs2_condition)]
            remove_prefix_conditions = conditions[-1]
            # print(f'{remove_prefix_conditions} fs2')
            hit_triggers = [ trigger for trigger in trigger_table.fs2_rows for condition in remove_prefix_conditions if condition in str(trigger.title) and trigger.description!=0 and trigger.description != ""]
            duplicate = []
            non_duplicate_hit_triggers = [hit_trigger for hit_trigger in hit_triggers if hit_trigger not in duplicate and not duplicate.append(hit_trigger)]
            # print(f'{rule.name} hit tirggers {non_duplicate_hit_triggers}')
            return_triggers[rule.name]+=[trigger.index for trigger in non_duplicate_hit_triggers if trigger.description != "" and str(trigger.description) != 'nan']
        ################### pj check ###########################################
        if( rule.pj_condition !='ー' and str(rule.pj_condition)!='nan'):
            print(f'{rule.name} pj')
            conditions = rule.pj_condition
            hit_triggers=[]
            if('or' in str(conditions)):
                or_conditions = re.split("or",conditions)
                remove_space_or_conditions = [condition.replace(" ",'').replace('\u3000','') for condition in or_conditions]
                # print(f'or conditions {remove_space_or_conditions}')
                filled_conditions = [condition for condition in remove_space_or_conditions for trigger in trigger_table.pj_rows if condition in str(trigger.description)]
                # print(f'filled conditions : {filled_conditions}')
            elif('AND' in str(conditions)):
                and_conditions = re.split('AND',conditions)
                remove_space_and_conditions = [ condition.replace(" ",'').replace('\u3000','') for condition in and_conditions]
                # print(f'pj conditins contain AND:{remove_space_and_conditions}')
                filled_conditions = [condition for condition in remove_space_and_conditions for trigger in trigger_table.pj_rows if condition in str(trigger.description)] 
                # print(f'filled condition :{filled_conditions}')
            else:
                # print(f'pj conditions do not contain some logical operator : {conditions}')
                hit_triggers = [trigger for trigger in trigger_table.pj_rows if conditions in str(trigger.description)]
                # print(f'hit triggers : {hit_triggers}')
            duplicate = []
            non_duplicate_hit_triggers = [hit_trigger for hit_trigger in hit_triggers if hit_trigger not in duplicate and not duplicate.append(hit_trigger)]
            return_triggers[rule.name]+=[trigger.index for trigger in non_duplicate_hit_triggers]
            if('No20' in rule.name and len(return_triggers[rule.name])==0):
                return_triggers[rule.name]=[]
            elif("No20" in rule.name and len(non_duplicate_hit_triggers)==0):
                return_triggers[rule.name]=[]
                
        ################# ot condition ####################################################
        if(rule.ot_condition != 'ー' and str(rule.ot_condition)!='nan'):
            hit_triggers=[]
            splitted_conditions = [condition.replace('\u3000','').replace('\n','').replace(" ",'') for condition in re.split("：|:",str(rule.ot_condition))]
            ab_splitted_conditions = [re.split('〈',c.replace('〉','')) if '〈' in str(c) else c for c in splitted_conditions]
            flatten_conditions=list(flatten_list(ab_splitted_conditions))
            trigger_rows = trigger_table.ot_rows
            for condition in flatten_conditions:
                if('and' in condition):
                    and_conditions = re.split('and',condition)
                    hit_triggers = []
                    for c in and_conditions:
                        trigger_rows = list_filter(c,trigger_rows)
                        indexes = [trigger.index for trigger in trigger_rows]
                        if(len(trigger_rows)==0):
                            hit_triggers=[]
                        else:
                            hit_triggers+=trigger_rows 
                elif('or' in condition):
                    or_conditions = re.split('or',condition)
                    hit_triggers = []
                    for c in or_conditions:
                        or_hit_trigger_rows=[]
                        if(len(list_filter(c,trigger_rows))>0):
                            or_hit_trigger_rows = list_filter(c,trigger_rows)
                        indexes = [trigger.index for trigger in or_hit_trigger_rows]
                        hit_triggers += or_hit_trigger_rows
                        # print(f'hit trigger {hit_triggers}')
                else:
                    trigger_rows = list_filter(condition,trigger_rows)
                    indexes = [trigger.index for trigger in trigger_rows]
                    hit_triggers=trigger_rows
            duplicate = []
            non_duplicate_hit_triggers = [hit_trigger for hit_trigger in hit_triggers if hit_trigger not in duplicate and not duplicate.append(hit_trigger)]
            return_triggers[rule.name]+=[trigger.index for trigger in non_duplicate_hit_triggers] 
        ################ in condition #######################################
        if(rule.in_condition != 'ー' and str(rule.in_condition)!='nan'):
            hit_triggers=[]
            if('and' in rule.in_condition or 'AND' in rule.in_condition):
                and_splitted_conditions=re.split('and|AND',str(rule.in_condition))
                and_conditions = [condition.replace(" ",'') for condition in and_splitted_conditions]
                condition_num = len(and_conditions)
                # print(f'and conditions {and_conditions},condition num {condition_num}')
                splitted_and_conditions = [re.split("：|:",condition) for condition in and_conditions]
                # print(f'splitted and conditions {splitted_and_conditions}')
                check_need_triggers = [trigger for trigger in trigger_table.in_rows for condition in splitted_and_conditions if condition[0] in str(trigger.title) and condition[1] in str(trigger.description) ]
                hit_triggers_num = len(check_need_triggers)
                # print(f'check need triggers {check_need_triggers},hit triggers num {hit_triggers_num}')
                if(condition_num == hit_triggers_num):
                    hit_triggers = check_need_triggers
            elif('or' in rule.in_condition or 'OR' in rule.in_condition):
                or_splitted_conditions=re.split('or|OR',str(rule.in_condition))
                or_conditions = [condition.replace(" ",'').replace('\u3000','') for condition in or_splitted_conditions]
                condition_num = len(or_conditions)
                # print(f'or conditions {or_conditions},condition num {condition_num}')
                splitted_or_conditions = [re.split("：|:",condition) for condition in or_conditions]
                # print(f'splitted or conditions {splitted_or_conditions}')
                check_need_triggers = [trigger for trigger in trigger_table.in_rows for condition in splitted_or_conditions if condition[0] in str(trigger.title) and condition[1] in str(trigger.description) ]
                hit_triggers_num = len(check_need_triggers)
                # print(f'check need triggers {check_need_triggers}')
                if(hit_triggers_num>0):
                    hit_triggers = check_need_triggers
            else:    
                splitted_conditions=re.split("：|:",str(rule.in_condition))
                # print(f'conditions:{splitted_conditions}')
                check_item = splitted_conditions[0]
                check_content = splitted_conditions[-1]
                hit_triggers = [trigger for trigger in trigger_table.in_rows if check_item in str(trigger.title) and check_content in str(trigger.description)]
                # print(f'hit triggers is {hit_triggers}')
            duplicate = []
            non_duplicate_hit_triggers = [hit_trigger for hit_trigger in hit_triggers if hit_trigger not in duplicate and not duplicate.append(hit_trigger)]
            return_triggers[rule.name]+=[trigger.index for trigger in non_duplicate_hit_triggers]
            
        ######################### 各ルールカラム間のAnd要件判定 ##############################################
        #24,45,94,137
    return [return_triggers]