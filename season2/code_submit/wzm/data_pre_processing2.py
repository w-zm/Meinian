DataWorks
prj_tc_231654_144246_qyyclr
数据集成数据开发数据管理运维中心项目管理机器学习平台
中文
TIANCHI_5532
任务开发
脚本开发
资源管理
函数管理
表查询
  任务开发
  all_gbdt_model
  code_submit
  Juzstu
  data_pre_process
 data_merge_split 可编辑
 get_num_features TIANCHI_5530 锁定 2018-05-28 23:10:23
 get_word_features TIANCHI_5530 锁定 2018-05-29 23:02:18
 origin_part1_part2_row2col TIANCHI_5530 锁定 2018-06-07 15:22:00
  every_predict_model
 dia_gbdt_best_rounds TIANCHI_5530 锁定 2018-06-02 10:40:16
 hdl_gbdt_best_rounds TIANCHI_5530 锁定 2018-06-02 10:59:10
 ldl_gbdt_best_rounds TIANCHI_5530 锁定 2018-06-02 10:58:53
 sys_gbdt_best_rounds TIANCHI_5530 锁定 2018-06-02 10:23:35
 tl_gbdt_best_rounds TIANCHI_5530 锁定 2018-06-02 19:41:41
  feature_selection
 classification_tl TIANCHI_5530 锁定 2018-05-18 17:13:59
 gbdt_log_model TIANCHI_5530 锁定 2018-06-07 15:45:32
 get_best_rounds TIANCHI_5530 锁定 2018-06-02 10:24:05
 predict_value_tl_gt_4 TIANCHI_5530 锁定 2018-05-20 11:10:36
 snp_drop_one_hot TIANCHI_5530 锁定 2018-06-07 15:44:28
  xgb_model_stacking
 add_prefix_for_xgb_model TIANCHI_5530 锁定 2018-06-07 16:12:01
 baseline_xgboost_jz TIANCHI_5530 锁定 2018-05-16 09:11:13
 calc_xgb_test_loss_and_save TIANCHI_5530 锁定 2018-06-07 16:07:15
 split_5_fold_data_xgb TIANCHI_5530 锁定 2018-06-07 16:02:16
 submit_result TIANCHI_5530 锁定 2018-05-31 11:29:28
  Tesla
 txt_feature_process TIANCHI_3597 锁定 2018-06-07 23:47:12
  wzm
 data_pre_processing1 我锁定 2018-06-08 20:48:48
 data_pre_processing2 我锁定 2018-06-08 21:17:58
 generate_dataset 我锁定 2018-06-08 21:21:28
 generate_test_b_table 我锁定 2018-06-09 15:54:03
 code_doc 可编辑
  Juzstu
  Jz_final
  new_tesla
  single_gbdt
  Tesla
  tmp_deal
  wzm
Loading...
新建 
保存
提交
测试运行
全屏
前往运维
data_pre_processing2
data_pre_processing1
txt_feature_process
submit_result
split_5_fold_data_xgb
calc_xgb_test_loss_and_save
baseline_xgboost_jz
add_prefix_for_xgb_model
snp_drop_one_hot
predict_value_tl_gt_4
get_best_rounds
gbdt_log_model
classification_tl

运行
停止
格式化
成本估计
 
1
##################################################################################
2
#                                                                                #
3
#            最终生成两个数据表wzm_trainset3_3_b和wzm_testset3_3_b               #
4
#                                                                                #
5
##################################################################################
6
​
7
from sklearn import preprocessing
8
import time
9
import pandas as pd
10
from odps import ODPS
11
from odps.df import DataFrame
12
import numpy as np
13
import re
14
import sys  
15
reload(sys)  
16
sys.setdefaultencoding('utf8')
17
​
18
def comp3(x):
19
    x = str(x)
20
    s = x.split(' ')
21
    if len(s)==3:
22
        if (my_is_float(s[0])==0)&(my_is_float(s[1])==0)&(my_is_float(s[2])==0):
23
            return (float(s[0]) + float(s[1]) + float(s[2]))/3
24
        else:
25
            return x
26
    else:
27
        return x 
28
    
29
def my_is_float(x):
30
    value = re.compile(r'^[-+]?[0-9]*[\.]?[0-9]+$')
31
    result = value.match(str(x))
32
    if result:
33
        return 0
34
    else:
35
        return 1
36
​
37
# 清洗：'7.42 7.42'
38
def comp(x):
39
    x = str(x)
40
    s = x.split(' ')
41
    if len(s)==2:
42
        if (my_is_float(s[0])==0)&(my_is_float(s[1])==0):
43
            return (float(s[0]) + float(s[1]))/2
44
        else:
45
            return x
46
    else:
47
        return x
48
    
调度配置
参数配置
 
