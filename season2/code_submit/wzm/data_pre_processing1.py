##################################################################################
#                                                                                #
#             最终生成两个数据表wzm_trainset3_3_b2和wzm_testset3_3_b2            #
#																				 #
##################################################################################

from sklearn import preprocessing
import time
import pandas as pd
from odps import ODPS
from odps.df import DataFrame
import numpy as np
import re
import sys  
reload(sys)  
sys.setdefaultencoding('utf8')

def comp3(x):
    x = str(x)
    s = x.split(' ')
    if len(s)==3:
        if (my_is_float(s[0])==0)&(my_is_float(s[1])==0)&(my_is_float(s[2])==0):
            return (float(s[0]) + float(s[1]) + float(s[2]))/3
        else:
            return x
    else:
        return x 
    
def my_is_float(x):
    value = re.compile(r'^[-+]?[0-9]*[\.]?[0-9]+$')
    result = value.match(str(x))
    if result:
        return 0
    else:
        return 1

# 清洗：'7.42 7.42'
def comp(x):
    x = str(x)
    s = x.split(' ')
    if len(s)==2:
        if (my_is_float(s[0])==0)&(my_is_float(s[1])==0):
            return (float(s[0]) + float(s[1]))/2
        else:
            return x
    else:
        return x
    
# 清洗：'nan 7.42'   
def comp2_3(x):
    x = str(x)
    s = x.split(' ')
    if len(s)==2:
        if (my_is_float(s[1])==0):
            return s[1] 
        elif (my_is_float(s[0])==0):
            return s[0]
        else:
            return x
    else:
        return x
    
def comp_other(x):
    if (x=='弃查')|(x=='nan')|(x=='None'):
        return -999
    else:
        return x
    
def strQ2B(ustring):  
    """全角转半角""" 
    ustring = str(ustring)
    rstring = ""  
    for uchar in ustring:  
        inside_code=ord(uchar)  
        if inside_code == 12288:                                            
            inside_code = 32   
        elif (inside_code >= 65281 and inside_code <= 65374):
            inside_code -= 65248  
  
        rstring += chr(inside_code)
    return rstring  

# 读取数据
part_1 = odps.get_table('meinian_round2_data_part1').to_df().to_pandas()
part_2 = odps.get_table('meinian_round2_data_part2').to_df().to_pandas()
part_1_2 = pd.concat([part_1,part_2])
part_1_2 = pd.DataFrame(part_1_2).sort_values('vid').reset_index(drop=True)
begin_time = time.time()
print('begin')
# 重复数据的拼接操作
def merge_table(df):
    df['results'] = df['results'].astype(str)
    if df.shape[0] > 1:
        merge_df = " ".join(list(df['results']))
    else:
        merge_df = df['results'].values[0]
    return merge_df
# 数据简单处理
print('find_is_copy')
print(part_1_2.shape)
is_happen = part_1_2.groupby(['vid','test_id']).size().reset_index()
# 重塑index用来去重
is_happen['new_index'] = is_happen['vid'] + '_' + is_happen['test_id']
is_happen_new = is_happen[is_happen[0]>1]['new_index']

part_1_2['new_index'] = part_1_2['vid'] + '_' + part_1_2['test_id']

unique_part = part_1_2[part_1_2['new_index'].isin(list(is_happen_new))]
unique_part = unique_part.sort_values(['vid','test_id'])
no_unique_part = part_1_2[~part_1_2['new_index'].isin(list(is_happen_new))]
print('begin')
part_1_2_not_unique = unique_part.groupby(['vid','test_id']).apply(merge_table).reset_index()
part_1_2_not_unique.rename(columns={0:'results'},inplace=True)
print('xxx')
tmp = pd.concat([part_1_2_not_unique,no_unique_part[['vid','test_id','results']]])
# 行列转换
print('finish')
tmp = tmp.pivot(index='vid',values='results',columns='test_id')
tmp.to_csv('./wzm_tmp.csv')
print(tmp.shape)
print('totle time',time.time() - begin_time)

a = pd.read_csv('./wzm_tmp.csv')

train = odps.get_table('meinian_round2_train').to_df().to_pandas()
test = odps.get_table('meinian_round2_test_b').to_df().to_pandas()

train_merge = pd.merge(train, a, on=['vid'], how='left')
test_merge = pd.merge(test, a, on=['vid'], how='left')

for i in train_merge.columns:
    train_merge[i] = train_merge[i].apply(strQ2B)
    test_merge[i] = test_merge[i].apply(strQ2B)

for i in train_merge.columns:
    train_merge[i] = train_merge[i].apply(comp)
    test_merge[i] = test_merge[i].apply(comp)
    train_merge[i] = train_merge[i].apply(comp2_3)
    test_merge[i] = test_merge[i].apply(comp2_3)
    
for i in train_merge.columns:
    train_merge[i] = train_merge[i].apply(lambda x: str(x).strip())
    test_merge[i] = test_merge[i].apply(lambda x: str(x).strip())

for i in train_merge.columns:
    train_merge[i] = train_merge[i].apply(lambda x: np.nan if x=='nan' else x)
    test_merge[i] = test_merge[i].apply(lambda x: np.nan if x=='nan' else x)
    
for i in train_merge.columns:
    train_merge[i] = train_merge[i].apply(comp_other)
    test_merge[i] = test_merge[i].apply(comp_other)
    
train_df = pd.DataFrame(train_merge.isnull().sum().sort_values())
test_df = pd.DataFrame(test_merge.isnull().sum().sort_values())

print(train_df.shape)
print(test_df.shape)

train_df = train_df.reset_index()
train_df.columns = ['f_name', 'nan_count']
test_df = test_df.reset_index()
test_df.columns = ['f_name', 'nan_count']

all_df = pd.concat([train_merge, test_merge], axis=0)
print('all_df')
print(all_df.shape)

all_count = pd.DataFrame(all_df.isnull().sum().sort_values())
all_count = all_count.reset_index()
all_count.columns = ['f_name', 'nan_count']
print(len(list(all_count[(all_count['nan_count']>=3655.8)]['f_name'])))

'''
删除缺失值超过或等于3655.8的特征列
'''
drop_lar_than_1000 = list(all_count[(all_count['nan_count']>=3655.8)]['f_name'])

print(len(drop_lar_than_1000))
train_merge = train_merge.drop(drop_lar_than_1000, axis=1)
test_merge = test_merge.drop(drop_lar_than_1000, axis=1)
all_df = all_df.drop(drop_lar_than_1000, axis=1)

not_feat_cols = ['vid', 'sys', 'dia', 'tl', 'hdl', 'ldl']

all_feat_cols = [i for i in all_df.columns if i not in not_feat_cols]

# 没有函数nunique()，只能手动统计唯一值
dit = {}
for i in all_df[all_feat_cols].columns:
    dit[i] = len(all_df[i].unique())

cate_pd = pd.DataFrame({'feat_name': dit.keys(), 'unq_count': dit.values()})

cate_cols = ['0979','0977','0980','3400','3197','0976','3399','0430','0212','0216','0432','0207','0206','0407','0406','0413','3191','3192','0433','0431','1304','3207','0420','1313','0423','2302','3430','0901','100010']

train_merge = train_merge.fillna(-999)
test_merge = test_merge.fillna(-999)

cate_cols2 = ['0435', '1328', '0405', '3196', '0425', '0201', '1305', '3195', '1303', '3190', '0707', '1315', '0973', '3730']

# 处理新增类型值特征2
train_merge.loc[train_merge['0435']=='正常', '0435'] = '未见异常'
train_merge.loc[train_merge['0435']=='未见明显异常', '0435'] = '未见异常'
train_merge.loc[train_merge['0435']=='左下腹部有压痛', '0435'] = '有压痛'
train_merge.loc[train_merge['0435']=='上腹部有压痛', '0435'] = '有压痛'
train_merge.loc[train_merge['0435']=='右外腹部有压痛', '0435'] = '有压痛'
train_merge.loc[train_merge['0435']=='右下腹部有压痛', '0435'] = '有压痛'

test_merge.loc[test_merge['0435']=='未见明显异常', '0435'] = '未见异常'
test_merge.loc[test_merge['0435']=='正常', '0435'] = '未见异常'
test_merge.loc[test_merge['0435']=='脐周腹部有压痛', '0435'] = '有压痛'
test_merge.loc[test_merge['0435']=='脐周腹紧张, 上腹腹壁紧张', '0435'] = '腹壁紧张触诊不满意'

train_merge.loc[train_merge['1328']=='正常', '1328'] = '未见异常'
train_merge.loc[train_merge['1328']=='未见异常 未见异常', '1328'] = '未见异常'
train_merge.loc[train_merge['1328']=='正常 正常 正常', '1328'] = '未见异常'
train_merge.loc[train_merge['1328']=='正常 正常 正常 正常', '1328'] = '未见异常'
train_merge.loc[train_merge['1328']=='色觉正常', '1328'] = '未见异常'
train_merge.loc[train_merge['1328']=='正常 正常', '1328'] = '未见异常'
train_merge.loc[train_merge['1328']=='双眼色弱', '1328'] = '色弱'

test_merge.loc[test_merge['1328']=='正常', '1328'] = '未见异常'
test_merge.loc[test_merge['1328']=='未见异常 未见异常', '1328'] = '未见异常'
test_merge.loc[test_merge['1328']=='未见异常 未见异常 未见异常', '1328'] = '未见异常'
test_merge.loc[test_merge['1328']=='正常 正常', '1328'] = '未见异常'
test_merge.loc[test_merge['1328']=='色觉：正常。', '1328'] = '未见异常'
test_merge.loc[test_merge['1328']=='正常 正常 正常 正常', '1328'] = '未见异常'
test_merge.loc[test_merge['1328']=='双眼色弱', '1328'] = '色弱'

train_merge.loc[train_merge['0405']=='双胸部哮鸣音未见异常', '0405'] = '未见异常'
train_merge.loc[train_merge['0405']=='双肺呼吸音正常，无干湿罗音', '0405'] = '未见异常'
train_merge.loc[train_merge['0405']=='右上肺部干啰音', '0405'] = '异常'
train_merge.loc[train_merge['0405']=='右肺尖哮鸣音', '0405'] = '异常'
train_merge.loc[train_merge['0405']=='双侧胸部哮鸣音', '0405'] = '异常'
train_merge.loc[train_merge['0405']=='右侧胸部哮鸣音', '0405'] = '异常'

test_merge.loc[test_merge['0405']=='未闻及', '0405'] = '未见异常'

train_merge.loc[train_merge['3196']=='阴性', '3196'] = '-'
train_merge.loc[train_merge['3196']=='正常', '3196'] = '-'

test_merge.loc[test_merge['3196']=='阴性', '3196'] = '-'
test_merge.loc[test_merge['3196']=='正常', '3196'] = '-'
test_merge.loc[test_merge['3196']=='Normal', '3196'] = '-'

train_merge.loc[train_merge['0425']=='正常', '0425'] = '未见异常'

test_merge.loc[test_merge['0425']=='正常', '0425'] = '未见异常'

train_merge.loc[train_merge['0201']=='正常', '0201'] = '未见异常'
train_merge.loc[train_merge['0201']=='未见明显异常', '0201'] = '未见异常'
train_merge.loc[train_merge['0201']=='右耳先天性耳前瘘管', '0201'] = '瘘管'
train_merge.loc[train_merge['0201']=='双耳轮脚前瘘管口', '0201'] = '瘘管'
train_merge.loc[train_merge['0201']=='左耳先天性耳前瘘管', '0201'] = '瘘管'
train_merge.loc[train_merge['0201']=='左耳轮脚前瘘管口', '0201'] = '瘘管'
train_merge.loc[train_merge['0201']=='左耳有付耳', '0201'] = '有付耳'
train_merge.loc[train_merge['0201']=='右副耳', '0201'] = '有付耳'

test_merge.loc[test_merge['0201']=='正常', '0201'] = '未见异常'
train_merge.loc[train_merge['0201']=='左耳先天性耳前瘘管', '0201'] = '瘘管'
train_merge.loc[train_merge['0201']=='双耳先天性耳前瘘管', '0201'] = '瘘管'
train_merge.loc[train_merge['0201']=='双侧耳轮脚前瘘管口', '0201'] = '瘘管'
train_merge.loc[train_merge['0201']=='右耳先天性耳前瘘管', '0201'] = '瘘管'
train_merge.loc[train_merge['0201']=='左耳轮脚前瘘管口', '0201'] = '瘘管'

train_merge.loc[train_merge['1305']=='正常', '1305'] = '未见异常'
train_merge.loc[train_merge['1305']=='正常 正常', '1305'] = '未见异常'
train_merge.loc[train_merge['1305']=='未见异常 未见异常', '1305'] = '未见异常'
train_merge.loc[train_merge['1305']=='左眼角膜云翳', '1305'] = '角膜云翳'
train_merge.loc[train_merge['1305']=='右角膜云翳：角膜处可见小片状淡灰色云翳 ', '1305'] = '角膜云翳'
train_merge.loc[train_merge['1305']=='右眼角膜斑翳', '1305'] = '角膜云翳'
test_merge.loc[test_merge['1305']=='右眼角膜白斑', '1305'] = '白斑'

test_merge.loc[test_merge['1305']=='正常', '1305'] = '未见异常'
test_merge.loc[test_merge['1305']=='左角膜见一黄棕色异物嵌入', '1305'] = '角膜云翳'
test_merge.loc[test_merge['1305']=='双眼先天性角膜变性', '1305'] = '角膜云翳'
test_merge.loc[test_merge['1305']=='正常 正常', '1305'] = '未见异常'
test_merge.loc[test_merge['1305']=='未见异常 未见异常', '1305'] = '未见异常'
test_merge.loc[test_merge['1305']=='左眼角膜斑翳', '1305'] = '角膜云翳'
test_merge.loc[test_merge['1305']=='右眼角膜云翳', '1305'] = '角膜云翳'
test_merge.loc[test_merge['1305']=='左眼角膜白斑', '1305'] = '白斑'
test_merge.loc[test_merge['1305']=='可见角膜接触镜', '1305'] = '双近视矫正术后'

train_merge.loc[train_merge['3195']=='阴性', '3195'] = '-'
train_merge.loc[train_merge['3195']=='阳性(+)', '3195'] = '+'

test_merge.loc[test_merge['3195']=='阴性', '3195'] = '-'
test_merge.loc[test_merge['3195']=='0(-)', '3195'] = '-'
test_merge.loc[test_merge['3195']=='-           0g/L', '3195'] = '-'

train_merge.loc[train_merge['1303']=='正常 正常', '1303'] = '正常'
train_merge.loc[train_merge['1303']=='正常 正常 正常', '1303'] = '正常'
train_merge.loc[train_merge['1303']=='正常 正常 正常 正常', '1303'] = '正常'
train_merge.loc[train_merge['1303']=='活动自如', '1303'] = '正常'
train_merge.loc[train_merge['1303']=='未见明显异常', '1303'] = '未见异常'
train_merge.loc[train_merge['1303']=='未见异常 未见异常', '1303'] = '未见异常'
train_merge.loc[train_merge['1303']=='左眼斜视, 左眼球萎缩', '1303'] = '异常'
train_merge.loc[train_merge['1303']=='双眼球突出', '1303'] = '异常'
train_merge.loc[train_merge['1303']=='左眼斜视', '1303'] = '异常'
train_merge.loc[train_merge['1303']=='左义眼', '1303'] = '异常'
train_merge.loc[train_merge['1303']=='双眼球震颤', '1303'] = '异常'

test_merge.loc[test_merge['1303']=='未见明显异常', '1303'] = '未见异常'
test_merge.loc[test_merge['1303']=='正常 正常', '1303'] = '正常'
test_merge.loc[test_merge['1303']=='未见异常 未见异常', '1303'] = '未见异常'
test_merge.loc[test_merge['1303']=='正常 正常 正常 正常', '1303'] = '正常'

train_merge.loc[train_merge['3190']=='阴性', '3190'] = '-'
train_merge.loc[train_merge['3190']=='3+', '3190'] = '+++'
train_merge.loc[train_merge['3190']=='60', '3190'] = '++++'

test_merge.loc[test_merge['3190']=='阴性', '3190'] = '-'
test_merge.loc[test_merge['3190']=='0(-)', '3190'] = '-'
test_merge.loc[test_merge['3190']=='-        0mmol/L', '3190'] = '-'

train_merge.loc[train_merge['0707']=='未见明显异常', '0707'] = '未见异常'
train_merge.loc[train_merge['0707']=='右侧颊粘膜溃疡', '0707'] = '溃疡'
train_merge.loc[train_merge['0707']=='疱疹性口炎', '0707'] = '溃疡'
train_merge.loc[train_merge['0707']=='上皮角化过度', '0707'] = '溃疡'

test_merge.loc[test_merge['0707']=='未见明显异常', '0707'] = '未见异常'
test_merge.loc[test_merge['0707']=='口角炎', '0707'] = '溃疡'

train_merge.loc[train_merge['1315']=='正常', '1315'] = '未见异常'
train_merge.loc[train_merge['1315']=='正常 正常', '1315'] = '未见异常'
train_merge.loc[train_merge['1315']=='未见异常 未见异常', '1315'] = '未见异常'
train_merge.loc[train_merge['1315']=='本人自述右瞳孔变形', '1315'] = '变形'
train_merge.loc[train_merge['1315']=='双侧瞳孔不圆', '1315'] = '变形'
train_merge.loc[train_merge['1315']=='右眼外伤瞳孔变形移位', '1315'] = '变形'
train_merge.loc[train_merge['1315']=='右侧瞳孔变形', '1315'] = '变形'
train_merge.loc[train_merge['1315']=='左侧瞳孔变形', '1315'] = '变形'
train_merge.loc[train_merge['1315']=='右瞳孔变形', '1315'] = '变形'

test_merge.loc[test_merge['1315']=='正常', '1315'] = '未见异常'
test_merge.loc[test_merge['1315']=='正常 正常', '1315'] = '未见异常'
test_merge.loc[test_merge['1315']=='未见异常 未见异常', '1315'] = '未见异常'


# 处理类型值特征
train_merge.loc[train_merge['0216']=='正常', '0216'] = '未见异常'
train_merge.loc[train_merge['0216']=='未见明显异常', '0216'] = '未见异常'

test_merge.loc[test_merge['0216']=='正常', '0216'] = '未见异常'
test_merge.loc[test_merge['0216']=='未见明显异常', '0216'] = '未见异常'

train_merge.loc[train_merge['0207']=='正常', '0207'] = '未见异常'
train_merge.loc[train_merge['0207']=='未见明显异常', '0207'] = '未见异常'
train_merge.loc[train_merge['0207']=='左鼻翼下方见约1.5CM大小黑痣，表面光滑。', '0207'] = '双侧鼻前庭炎'

test_merge.loc[test_merge['0207']=='正常', '0207'] = '未见异常'
test_merge.loc[test_merge['0207']=='未见明显异常', '0207'] = '未见异常'

train_merge.loc[train_merge['0206']=='正常', '0206'] = '未见异常'
train_merge.loc[train_merge['0206']=='未见明显异常', '0206'] = '未见异常'

test_merge.loc[test_merge['0206']=='正常', '0206'] = '未见异常'
test_merge.loc[test_merge['0206']=='未见明显异常', '0206'] = '未见异常'

train_merge.loc[train_merge['0406']=='肋下未触及', '0406'] = '未触及'

test_merge.loc[test_merge['0406']=='肋下未触及', '0406'] = '未触及'

train_merge.loc[train_merge['0413']=='未见明显异常', '0413'] = '未见异常'
train_merge.loc[train_merge['0413']=='右下肢静脉曲张术后', '0413'] = '异常'
train_merge.loc[train_merge['0413']=='心动过缓', '0413'] = '异常'
train_merge.loc[train_merge['0413']=='有黑蒙.昏厥史，已通知急赴医院心内科进一步检查.', '0413'] = '异常'

test_merge.loc[test_merge['0413']=='原发性震颤', '0413'] = '异常'

train_merge.loc[train_merge['3191']=='阴性', '3191'] = '-'

test_merge.loc[test_merge['3191']=='阴性', '3191'] = '-'
test_merge.loc[test_merge['3191']=='0(-)', '3191'] = '-'

train_merge.loc[train_merge['3192']=='阴性', '3192'] = '-'

test_merge.loc[test_merge['3192']=='阴性', '3192'] = '-'
test_merge.loc[test_merge['3192']=='0(-)', '3192'] = '-'

train_merge.loc[train_merge['0433']=='未及', '0433'] = '未触及'

train_merge.loc[train_merge['0431']=='无压痛点', '0431'] = '无'
train_merge.loc[train_merge['0431']=='未触及', '0431'] = '未见异常'

test_merge.loc[test_merge['0431']=='无压痛点', '0431'] = '无'

train_merge.loc[train_merge['1304']=='未见异常 未见异常', '1304'] = '未见异常'
train_merge.loc[train_merge['1304']=='正常 正常 正常 正常', '1304'] = '未见异常'
train_merge.loc[train_merge['1304']=='正常 正常', '1304'] = '未见异常'
train_merge.loc[train_merge['1304']=='正常', '1304'] = '未见异常'

test_merge.loc[test_merge['1304']=='正常', '1304'] = '未见异常'
test_merge.loc[test_merge['1304']=='正常 正常', '1304'] = '未见异常'
test_merge.loc[test_merge['1304']=='未见异常 未见异常', '1304'] = '未见异常'
test_merge.loc[test_merge['1304']=='正常 正常 正常 正常', '1304'] = '未见异常'

train_merge.loc[train_merge['3207']=='+++草酸钙结晶', '3207'] = '查见'
train_merge.loc[train_merge['3207']=='阴性', '3207'] = '未见'

test_merge.loc[train_merge['3207']=='少量', '3207'] = '查见'
test_merge.loc[train_merge['3207']=='Ⅱ', '3207'] = '查见'
test_merge.loc[train_merge['3207']=='草酸钙结晶', '3207'] = '查见'

train_merge.loc[train_merge['0420']=='未闻及异常', '0420'] = '未见异常'
train_merge.loc[train_merge['0420']=='正常', '0420'] = '未见异常'

test_merge.loc[test_merge['0420']=='正常', '0420'] = '未见异常'
test_merge.loc[test_merge['0420']=='未闻及异常', '0420'] = '未见异常'
test_merge.loc[test_merge['0420']=='心音强', '0420'] = '强弱不等'

train_merge.loc[train_merge['1313']=='正常 正常', '1313'] = '未见异常'
train_merge.loc[train_merge['1313']=='正常', '1313'] = '未见异常'

test_merge.loc[test_merge['1313']=='正常', '1313'] = '未见异常'
test_merge.loc[test_merge['1313']=='正常 正常', '1313'] = '未见异常'
test_merge.loc[test_merge['1313']=='未见异常 未见异常', '1313'] = '未见异常'

train_merge.loc[train_merge['0423']=='左肺呼吸音粗', '0423'] = '双肺呼吸音粗'
train_merge.loc[train_merge['0423']=='右肺呼吸音减弱', '0423'] = '双肺呼吸音减弱'
train_merge.loc[train_merge['0423']=='正常', '0423'] = '未见异常'
train_merge.loc[train_merge['0423']=='未见明显异常', '0423'] = '未见异常'

test_merge.loc[test_merge['0423']=='未见明显异常', '0423'] = '未见异常'
test_merge.loc[test_merge['0423']=='正常', '0423'] = '未见异常'
test_merge.loc[test_merge['0423']=='右下肺呼吸音粗', '0423'] = '双肺呼吸音粗'

train_merge.loc[train_merge['2302']=='3117372011健康', '2302'] = '健康'
train_merge.loc[train_merge['2302']=='Y81Q004698健康', '2302'] = '健康'
train_merge.loc[train_merge['2302']=='115141548健康', '2302'] = '健康'

train_merge.loc[train_merge['3430']=='阴性', '3430'] = '-'
train_merge.loc[train_merge['3430']=='+-  Ca15  Leu/uL', '3430'] = '+-'
train_merge.loc[train_merge['3430']=='阳性(+)', '3430'] = '+'

test_merge.loc[test_merge['3430']=='阴性', '3430'] = '-'
test_merge.loc[test_merge['3430']=='0(-)', '3430'] = '-'
test_merge.loc[test_merge['3430']=='阳性(+)', '3430'] = '+'

train_merge.loc[train_merge['0901']=='正常', '0901'] = '未见异常'
train_merge.loc[train_merge['0901']=='未见明显异常', '0901'] = '未见异常'
train_merge.loc[train_merge['0901']=='色素沉着〔左前臂〕', '0901'] = '色素沉着'

test_merge.loc[test_merge['0901']=='正常', '0901'] = '未见异常'
test_merge.loc[test_merge['0901']=='未见异常 未见异常', '0901'] = '未见异常'

train_merge.loc[train_merge['100010']=='阴性', '100010'] = '-'
train_merge.loc[train_merge['100010']=='阳性(+)', '100010'] = '+'
train_merge.loc[train_merge['100010']=='阳性(1+)', '100010'] = '+'

test_merge.loc[test_merge['100010']=='0(-)', '100010'] = '-'
test_merge.loc[test_merge['100010']=='阴性', '100010'] = '-'

train_merge.loc[train_merge['0976']=='右下肢', '0976'] = '双下肢'

test_merge.loc[test_merge['0976']=='无 无', '0976'] = '无'

test_merge.loc[test_merge['3399']=='微红', '3399'] = '棕黄'

test_merge.loc[test_merge['3400']=='混浊', '3400'] = '微浑'
test_merge.loc[test_merge['3400']=='微混', '3400'] = '微浑'

all_df = pd.concat([train_merge, test_merge], axis=0)

for i in cate_cols2:
    le = preprocessing.LabelEncoder()
    le.fit(all_df[i])
    train_merge[i] = le.transform(train_merge[i])
    test_merge[i] = le.transform(test_merge[i])

for i in cate_cols:
    le = preprocessing.LabelEncoder()
    le.fit(all_df[i])
    train_merge[i] = le.transform(train_merge[i])
    test_merge[i] = le.transform(test_merge[i])
    
'''
import re
def my_is_float(x):
    value = re.compile(r'^[-+]?[0-9]*[\.]?[0-9]+$')
    result = value.match(str(x))
    if result:
        return 0
    else:
        return 1

exclude_cols = ['vid', 'sys', 'dia', 'tl', 'hdl', 'ldl']
cate_cols = ['0979','0977','0980','3400','3197','0976','3399','0430','0212','0216','0432','0207','0206','0407','0406','0413','3191','3192','0433','0431','1304','3207','0420','1313','0423','2302','3430','0901','100010']
#c_not_count = ['10004', '319', '313', '32', '320', '33', '312', '192', '1117', '183', '1814', '317', '191', '10003', '2403', '1115', '3193', '1840', '193', '2405', '37', '1850', '10002', '2174', '100006', '316', '31', '315', '2406', '1845', '38', '190', '2404']

e_c_cols = exclude_cols + cate_cols + cate_cols2
num_cols = [col for col in train_merge.columns if col not in e_c_cols]

for c in num_cols:
    train_merge[c+'_'+'counts'] = train_merge[c].apply(my_is_float)
    
num_count_cols = []
for i in num_cols:
    num_count_cols.append(i+'_counts')
    
num_df = pd.DataFrame(train_merge[num_count_cols].sum().sort_values()).reset_index()
num_df.columns = ['f_name', 'not_f_count']    
#print(list(num_df[num_df['not_f_count']<30]['f_name']))

c_not_count = ['10004', '319', '313', '32', '320', '33', '312', '192', '1117', '183', '1814', '317', '191', '10003', '2403', '1115', '3193', '1840', '193', '2405', '37', '1850', '10002', '2174', '100006', '316', '31', '315', '2406', '1845', '38', '190', '2404']+['100012', '669001', '269012', '269013', '269014', '269015', '669002', '669004', '669005', '669006', '669007', '669008', '669009', '669012', '2386', '709038', '709049', '809001', '809004', '809008', '809009', '809010', '809013', '809017', '100005', '100007', '100008', '659025', '2372', '2333', '300093', '300107', '300109', '300129', '1815', '269003', '1345', '269004', '269005', '809023', '269006', '269008', '34', '269009', '269010', '269011', '1127', '1112', '1107', '1106', '378', '39', '269007', '809025', '669021', '269017', '979011', '979012', '979013', '979014', '979015', '979016', '979017', '269016', '300017', '979018', '979019', '979020', '979021', '979022', '979023', '300021', '0112', '0111', '0109', '0108', '0106', '0105', '0104', '979009', '979008', '2420', '809026', '979007', '269025', '2986', '300001', '269024', '269019', '269023', '300008', '300011', '269020', '300012', '300013', '269022', '979004', '269021', '979005', '979006', '300014', '269018', '155', '20002', '139', '1110', '669003', '100013', '100014', '979003', 'A701', '2371', 'A703', '979002', '979001', '314', '1474', '143', '300035', '809021', '1873', '141']


for i in c_not_count:
    train_merge[i] = train_merge[i].apply(comp)
    test_merge[i] = test_merge[i].apply(comp)
    train_merge[i] = train_merge[i].apply(comp2_3)
    test_merge[i] = test_merge[i].apply(comp2_3)
    #train_merge[i] = train_merge[i].apply(comp2_3)
    test_merge[i] = test_merge[i].apply(comp3)

train_merge.loc[train_merge['2404']=='未查', '2404'] = -999
train_merge.loc[train_merge['2403']=='未查', '2403'] = -999

train_merge.loc[train_merge['3193']=='>=1.030', '3193'] = 1.030
test_merge.loc[test_merge['3193']=='<=1.005', '3193'] = 1.005
test_merge.loc[test_merge['3193']=='>=1.030', '3193'] = 1.030

train_merge.loc[train_merge['1112']=='5.5(%)', '1112'] = 5.5
test_merge.loc[test_merge['1112']=='5.5(%)', '1112'] = 5.5

train_merge.loc[train_merge['A703']=='234db/m(正常）', 'A703'] = 234

train_merge.loc[train_merge['155']=='详见报告单', '155'] = 2.35636227876
test_merge.loc[test_merge['155']=='<0.005', '155'] = 0.0025

train_merge.loc[train_merge['20002']=='详见报告单', '20002'] = 0.371647272727

train_merge.loc[train_merge['139']=='详见报告单', '139'] = 1.49015242881

train_merge.loc[train_merge['1110']=='1.2.', '1110'] = 1.2

train_merge.loc[train_merge['669003']=='<0.008', '669003'] = 0.0079

train_merge.loc[train_merge['100013']=='详见报告单', '100013'] = 4.33051754386

train_merge.loc[train_merge['100014']=='详见报告单', '100014'] = 15.118346087

train_merge.loc[train_merge['979003']=='-----', '979003'] = 10.1245179063

train_merge.loc[train_merge['A701']=='5.5kpa(正常）', 'A701'] = 5.5

train_merge.loc[train_merge['979002']=='-----', '979002'] = 0.176593220339

train_merge.loc[train_merge['979001']=='-----', '979001'] = 15.6934530387

train_merge.loc[train_merge['2371']=='阴性', '2371'] = 2.59352583587

train_merge.loc[train_merge['A703']=='234db/m(正常）', 'A703'] = 234

train_merge.loc[train_merge['314']=='.50.81', '314'] = 50.81

train_merge.loc[train_merge['1474']=='75.71.', '1474'] = 75.71
train_merge.loc[train_merge['1474']=='详见报告单', '1474'] = 92.1230054945

train_merge.loc[train_merge['143']=='详见报告单', '143'] = 14.1660686695
train_merge.loc[train_merge['143']=='>50.00', '143'] = 61.5236363636
test_merge.loc[test_merge['143']=='详见报告单', '143'] = 14.1660686695

train_merge.loc[train_merge['141']=='<3.00', '141'] = 2.12
test_merge.loc[test_merge['141']=='<3.00', '141'] = 2.12

train_merge.loc[train_merge['300035']=='<5', '300035'] = 3   #可能不对
train_merge.loc[train_merge['300035']=='<1', '300035'] = 0.548815384615   #可能不对
train_merge.loc[train_merge['300035']=='<1.00', '300035'] = 0.548815384615   #可能不对
test_merge.loc[test_merge['300035']=='<1.00', '300035'] = 0.548815384615

train_merge.loc[train_merge['809021']=='/', '809021'] = 7.73979710145
test_merge.loc[test_merge['809021']=='/', '809021'] = 7.73979710145

train_merge.loc[train_merge['1873']=='详见报告单', '1873'] = 1.94921846154
train_merge.loc[train_merge['1873']=='阴性', '1873'] = 1.94921846154
test_merge.loc[test_merge['1873']=='详见报告单', '1873'] = 1.94921846154

test_merge.loc[test_merge['300017']=='阴性', '300017'] = 1.26276977401

test_merge.loc[test_merge['300021']=='<3.4', '300021'] = 1.51711538462
test_merge.loc[test_merge['300021']=='<1', '300021'] = 0.424157894737

test_merge.loc[test_merge['155']=='详见报告单', '155'] = 2.35636227876

test_merge.loc[test_merge['20002']=='详见报告单', '20002'] = 0.371647272727

test_merge.loc[test_merge['139']=='详见报告单', '139'] = 1.49015242881

test_merge.loc[test_merge['A701']=='3.0kpa(正常）', 'A701'] = 3.0

test_merge.loc[test_merge['2371']=='<0.3', '2371'] = 0.188888888889

test_merge.loc[test_merge['A703']=='208db/m(正常）', 'A703'] = 208

test_merge.loc[test_merge['1474']=='详见报告单', '1474'] = 92.1230054945

test_merge.loc[test_merge['300035']=='<1.0', '300035'] = 0.548815384615   #可能不对

def my_avg(x):
    value = re.compile(r'^[-+]?[0-9]*[\.]?[0-9]+$')
    result = value.match(str(x))
    if result:
        if (float(x) >=0)&(float(x) <=1.005):
            return float(x)
        else:
            return -100
    else:
        return -100

l = list(train_merge['3193'])
ll = [my_avg(x) for x in l]
lll = [x for x in ll if x > 0]

print(sum(lll) / len(lll))

for i in c_not_count:
    print(i)
    train_merge[i] = train_merge[i].astype('float')
    test_merge[i] = test_merge[i].astype('float')

for i in c_not_count:
    train_merge[i] = train_merge[i].replace(-999, np.nan)
    test_merge[i] = test_merge[i].replace(-999, np.nan)

# 增加额外的特征table_count、result_len和result_avg_len
part_1 = odps.get_table('meinian_round2_data_part1').to_df().to_pandas()
part_2 = odps.get_table('meinian_round2_data_part2').to_df().to_pandas()
part_1_2 = pd.concat([part_1,part_2])

part_gr = part_1_2[['vid', 'test_id']].groupby(['vid']).count()

part_gr = part_gr.reset_index()
part_gr.columns = ['vid', 'table_count']

part_1_2['result_str'] = part_1_2['results'].astype('str')
part_1_2['result_len'] = part_1_2['result_str'].apply(lambda x: len(x))

part_gr['result_len'] = part_1_2[['vid', 'test_id', 'result_len']].groupby(['vid']).sum().reset_index()['result_len']

train_merge = pd.merge(train_merge, part_gr, on=['vid'], how='left')
test_merge = pd.merge(test_merge, part_gr, on=['vid'], how='left')

train_merge['result_avg_len'] = train_merge['result_len'] / train_merge['table_count']
test_merge['result_avg_len'] = test_merge['result_len'] / test_merge['table_count']
'''
# 生成tmp2.csv，将所有的报告结果以‘_’连接起来，然后提取特征sugar_high_related以及sex
concat_result = pd.read_csv('./wzm_tmp.csv')

cols = [i for i in concat_result.columns if i not in ['vid']]
concat_result['all_result'] = '_'

for i in cols:
    concat_result['all_result'] = concat_result['all_result'] +  '_' + concat_result[i].astype('str') 

def is_sh_related(x):
    x = str(x)
    if ('高血压' in x) | ('血脂' in x) | ('糖尿病' in x) |('妊娠' in x) | ('肝炎' in x) | ('动脉硬化' in x)|('肾病综合症' in x)|('心肌梗死' in x)| ('脂肪肝' in x) | ('胰岛' in x) |('冠心病' in x)|('贫血' in x)|('胰腺炎' in x)|('血压偏高' in x)|('肥胖' in x):
        return 1
    else:
        return 0

def is_sex(x):
    x = str(x)
    if ('阴道' in x)|('子宫' in x)|('妇' in x)|('乳房' in x)|('孕' in x)|('阴道' in x)|('女' in x)|('宫颈' in x)|('妊娠' in x)|('剖腹产' in x)|('外阴' in x):
        return 1
    elif ('前列腺' in x)|('包皮' in x)|('包茎' in x)|('男' in x)|('吸烟' in x):
        return 2
    else:
        return 0
    
concat_result['sugar_high_related'] = concat_result['all_result'].apply(is_sh_related)
concat_result['sex'] = concat_result['all_result'].apply(is_sex)

train_merge = pd.merge(train_merge, concat_result[['vid', 'sugar_high_related', 'sex']], on=['vid'], how='left')
test_merge = pd.merge(test_merge, concat_result[['vid', 'sugar_high_related', 'sex']], on=['vid'], how='left')
'''
# 修正身高，体重，bmi
c1 = train_merge.loc[(train_merge['2403']==-999)&(train_merge['2404']!=-999)&(train_merge['2405']!=-999), '2403'].index.values
c2 = train_merge.loc[(train_merge['2403']!=-999)&(train_merge['2404']==-999)&(train_merge['2405']!=-999), '2404'].index.values
c3 = train_merge.loc[(train_merge['2403']!=-999)&(train_merge['2404']!=-999)&(train_merge['2405']==-999), '2405'].index.values

for i in c1:
    train_merge.loc[i, '2403'] = (train_merge.loc[i, '2404']/100) * (train_merge.loc[i, '2404']/100) * (train_merge.loc[i, '2405'])
    
for i in c2:
    train_merge.loc[i, '2404'] = np.sqrt((train_merge.loc[i, '2403']) / (train_merge.loc[i, '2405'])) * 100

for i in c3:
    train_merge.loc[i, '2405'] = (train_merge.loc[i, '2403']) / ((train_merge.loc[i, '2404']/100) * (train_merge.loc[i, '2404']/100))

train_merge.loc[train_merge['2403']==0, '2403'] = -999
train_merge.loc[train_merge['2404']==0, '2404'] = -999
train_merge.loc[train_merge['2405']==0, '2405'] = -999

c1_test = test_merge.loc[(test_merge['2403']==-999)&(test_merge['2404']!=-999)&(test_merge['2405']!=-999), '2403'].index.values
c2_test = test_merge.loc[(test_merge['2403']!=-999)&(test_merge['2404']==-999)&(test_merge['2405']!=-999), '2404'].index.values
c3_test = test_merge.loc[(test_merge['2403']!=-999)&(test_merge['2404']!=-999)&(test_merge['2405']==-999), '2405'].index.values

for i in c1_test:
    test_merge.loc[i, '2403'] = (test_merge.loc[i, '2404']/100) * (test_merge.loc[i, '2404']/100) * (test_merge.loc[i, '2405'])
    
for i in c2_test:
    test_merge.loc[i, '2404'] = np.sqrt((test_merge.loc[i, '2403']) / (test_merge.loc[i, '2405'])) * 100

for i in c3_test:
    test_merge.loc[i, '2405'] = (test_merge.loc[i, '2403']) / ((test_merge.loc[i, '2404']/100) * (test_merge.loc[i, '2404']/100))

test_merge.loc[test_merge['2403']==0, '2403'] = -999
test_merge.loc[test_merge['2404']==0, '2404'] = -999
test_merge.loc[test_merge['2405']==0, '2405'] = -999

t_train = DataFrame(train_merge[e_c_cols+c_not_count+['table_count','result_len','result_avg_len','sugar_high_related','sex']])
t_train.persist('wzm_trainset3_3_b2')
t_test = DataFrame(test_merge[e_c_cols+c_not_count+['table_count','result_len','result_avg_len','sugar_high_related','sex']])
t_test.persist('wzm_testset3_3_b2')
'''
exclude_cols = ['vid', 'sys', 'dia', 'tl', 'hdl', 'ldl']
e_c_cols = exclude_cols + cate_cols + cate_cols2
t_train = DataFrame(train_merge[e_c_cols+['sugar_high_related']])
t_train.persist('wzm_trainset3_3_b2')
t_test = DataFrame(test_merge[e_c_cols+['sugar_high_related']])
t_test.persist('wzm_testset3_3_b2')



