# README
****
### 团队：Unreal

### 初赛Rank2，复赛Rank10
****

### 文件夹说明：
- data:数据文件夹
- features: [juzstu](https://github.com/juzstu)手动整理好的数值型和文字型特征
- code: 主运行代码
- team: [juzstu](https://github.com/juzstu)的Mongodb操作代码
- submit: 提交结果文件夹

### 需要注意的点：

- code文件夹下共有三个代码文件，请按照main1.py，main2.ipynb，main3.ipynb的顺序**依次执行**，因为每一步的代码需要用到上一步所产生的中间结果。
- 简单说明每个文件的作用：
    - main1.py：模型1，5个lightgbm模型的平均加权
    - main2.ipynb：模型2，1个训练全数据集的xgboost模型
    - main3.ipynb：融合以上模型结果，融合策略：模型1'血清甘油三酯'大于5的值直接使用，不融合，其它直接1：1进行加权。
- 这是最原始的代码，没有经过整理，存在很多人工处理的部分，请见谅。 
